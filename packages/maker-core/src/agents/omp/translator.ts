/**
 * omp RPC 事件帧 → AgentEvent 翻译层。
 *
 * omp 的 `--mode rpc` 协议与 pi 的 JSONL 帧同构（wire-identical），但帧集不同：
 * omp 没有 message_end 全文校准轮，文本以 text_delta / text_end 结对上报，
 * 终态以 prompt_result + agent_end 落地。形状对齐 pi translator 消费方：
 *  - text:     { text, isFinal }
 *  - thinking: { stage, blockId, text, startedAt?, durationMs? }
 *  - tool_use: { toolUseId, toolName, input }
 *  - tool_result_full: { toolUseId, fullText, isError }
 *  - tool_result:      { summary, toolUseIds }
 *  - status:   { status, ...UsageSnapshot 展开, isRunning }
 *  - done:     { type: 'omp/agent_settled', status, usage }
 *
 * 未知事件类型只记 debug 并丢弃 —— omp 插件桥（extension_ui_request）等帧
 * 不进产品事件流，也不能让翻译器崩溃。
 */

import { parseMessageToolUse } from '@cindy/maker-shared/message-normalize';
import type { Logger } from '../../interfaces/logger.js';
import type { AgentEvent, UsageSnapshot } from '../../types/index.js';
import type { AsyncQueue } from '../shared/async-queue.js';
import type { PiRpcEvent } from '../pi/rpc-client.js';

/** omp prompt / turn 消息携带的 usage（形状同 pi：camelCase，reasoningTokens 为 reasoning）。 */
interface OmpUsage {
  input?: number;
  output?: number;
  cacheRead?: number;
  cacheWrite?: number;
  totalTokens?: number;
  reasoningTokens?: number;
  cost?: { total?: number };
}

/** 一轮 translator 状态：omp 一次 prompt = 一条 agent run，帧序列线性。 */
export interface OmpTranslateContext {
  logger: Logger;
  turnTokens: number;
  turnInput: number;
  turnOutput: number;
  turnCacheRead: number;
  turnCacheWrite: number;
  /** 最后一次 API call 的 context 占用（input + cacheRead + cacheWrite）。 */
  contextTokens: number;
  costUsd: number;
  isStreaming: boolean;
  turnSettled: boolean;
  finalAssistantText: string;
  finalAssistantStopReason: string | null;
  thinkingBlocks: Map<number, { blockId: string; startedAt: number }>;
  thinkingSeq: number;
}

export function createOmpTranslateContext(logger: Logger): OmpTranslateContext {
  return {
    logger,
    turnTokens: 0,
    turnInput: 0,
    turnOutput: 0,
    turnCacheRead: 0,
    turnCacheWrite: 0,
    contextTokens: 0,
    costUsd: 0,
    isStreaming: false,
    turnSettled: false,
    finalAssistantText: '',
    finalAssistantStopReason: null,
    thinkingBlocks: new Map(),
    thinkingSeq: 0,
  };
}

/** session 关闭时释放 translator 资源（omp 无 timer/监听，仅状态复位）。 */
export function disposeOmpTranslateContext(ctx: OmpTranslateContext): void {
  ctx.isStreaming = false;
  ctx.turnSettled = true;
  ctx.thinkingBlocks.clear();
}

/** 纯快照：读取不消费，close/teardown 可重试。omp 无 requestUsage/TPS 计量。 */
export function ompUsageSnapshotOf(ctx: OmpTranslateContext): UsageSnapshot {
  return {
    tokenUsage: ctx.turnTokens,
    contextTokens: ctx.contextTokens,
    // omp 的 contextWindow 不经 rpc 上报；0 = 未知（目录按模型前缀兜底）。
    contextWindow: 0,
    costUsd: ctx.costUsd,
    outputTokens: ctx.turnOutput,
  };
}

/** 帧字段收窄助手：RPC 帧是外部输入，先经 typeof/in 判读再取值。 */
function readFrameNumber(frame: Record<string, unknown>, key: string): number | undefined {
  return typeof frame[key] === 'number' ? frame[key] : undefined;
}

function readFrameString(frame: Record<string, unknown>, key: string): string | undefined {
  return typeof frame[key] === 'string' ? frame[key] : undefined;
}

/** 帧内嵌套对象收窄：in + typeof 双重判读后按具名形状读字段。 */
function frameRecord(frame: Record<string, unknown>, key: string): Record<string, unknown> | undefined {
  const value = frame[key];
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;
}

/** 解析 usage 对象；缺字段按 undefined（消费方 ?? 0 兜底）。 */
function usageOf(frame: Record<string, unknown>): OmpUsage | undefined {
  const raw = frameRecord(frame, 'usage');
  if (!raw) return undefined;
  const cost = frameRecord(raw, 'cost');
  return {
    input: readFrameNumber(raw, 'input'),
    output: readFrameNumber(raw, 'output'),
    cacheRead: readFrameNumber(raw, 'cacheRead'),
    cacheWrite: readFrameNumber(raw, 'cacheWrite'),
    totalTokens: readFrameNumber(raw, 'totalTokens'),
    reasoningTokens: readFrameNumber(raw, 'reasoningTokens'),
    cost: cost ? { total: readFrameNumber(cost, 'total') } : undefined,
  };
}

function applyUsage(ctx: OmpTranslateContext, usage: OmpUsage | undefined): void {
  if (!usage) return;
  const input = usage.input ?? 0;
  const output = usage.output ?? 0;
  const cacheRead = usage.cacheRead ?? 0;
  const cacheWrite = usage.cacheWrite ?? 0;
  ctx.turnTokens += input + output;
  ctx.turnInput += input;
  ctx.turnOutput += output;
  ctx.turnCacheRead += cacheRead;
  ctx.turnCacheWrite += cacheWrite;
  // 全 0 usage（中断收尾帧）不覆盖上一次真实占用。
  const requestContextTokens = input + cacheRead + cacheWrite;
  if (requestContextTokens > 0) ctx.contextTokens = requestContextTokens;
  const cost = usage.cost?.total;
  if (typeof cost === 'number' && Number.isFinite(cost)) ctx.costUsd += cost;
}

function pushStatus(
  queue: AsyncQueue<AgentEvent>,
  ctx: OmpTranslateContext,
  text: string,
  isRunning: boolean,
): void {
  queue.push({
    type: 'status',
    data: { status: text, ...ompUsageSnapshotOf(ctx), isRunning },
    source: 'omp',
  });
}

/** 主入口：一帧 omp RPC 事件 → 0..n 个 AgentEvent。 */
export function translateOmpEvent(
  event: PiRpcEvent,
  queue: AsyncQueue<AgentEvent>,
  ctx: OmpTranslateContext,
): void {
  switch (event.type) {
    case 'agent_start': {
      if (ctx.isStreaming && !ctx.turnSettled) {
        ctx.logger.warn('omp translator: agent_start inside an unsettled turn', {});
        return;
      }
      ctx.isStreaming = true;
      ctx.turnSettled = false;
      ctx.turnTokens = 0;
      ctx.turnInput = 0;
      ctx.turnOutput = 0;
      ctx.turnCacheRead = 0;
      ctx.turnCacheWrite = 0;
      ctx.finalAssistantText = '';
      ctx.finalAssistantStopReason = null;
      ctx.thinkingBlocks.clear();
      ctx.thinkingSeq = 0;
      pushStatus(queue, ctx, 'Working…', true);
      return;
    }

    // omp 的 turn_start / message_start / message_end 不携带产品事件（文本经
    // message_update 的增量流上报，message_end 重复同一份全文），忽略。
    case 'turn_start':
    case 'message_start':
    case 'message_end':
    case 'session_settled':
    case 'advisor_yielded':
    case 'extension_ui_request':
    case 'available_commands_update':
    case 'config_warnings_changed':
      return;

    case 'message_update': {
      const delta = frameRecord(event, 'assistantMessageEvent');
      if (delta && typeof delta.type === 'string') {
        handleAssistantDelta(delta, queue, ctx);
      }
      return;
    }

    case 'tool_execution_start': {
      const toolUseId = readFrameString(event, 'toolCallId') ?? '';
      const toolName = readFrameString(event, 'toolName') ?? 'tool';
      const toolArgs = typeof event.args === 'object' && event.args !== null ? event.args : {};
      queue.push({
        type: 'tool_use',
        data: {
          toolUseId,
          ...parseMessageToolUse({
            role: 'tool_use',
            content: { toolName, input: toolArgs },
            toolUseId,
          }),
        },
        source: 'omp',
      });
      pushStatus(queue, ctx, `Running ${toolName}…`, true);
      return;
    }

    case 'tool_execution_update':
      // omp 流式中间结果（partialResult）P0 不消费；终态经 tool_execution_end 上报。
      return;

    case 'tool_execution_end': {
      const toolUseId = readFrameString(event, 'toolCallId') ?? '';
      const isError = event.isError === true;
      queue.push({
        type: 'tool_result_full',
        data: { toolUseId, fullText: ompToolResultFullText(event.result), isError },
        source: 'omp',
      });
      queue.push({
        type: 'tool_result',
        data: { summary: isError ? 'failed' : 'done', toolUseIds: [toolUseId] },
        source: 'omp',
      });
      return;
    }

    case 'turn_end': {
      // turn_end.message 携带整轮 usage 与 stopReason；文本已由 delta 流上报。
      const message = frameRecord(event, 'message');
      if (message?.role === 'assistant') {
        applyUsage(ctx, usageOf(message));
        const stopReason = readFrameString(message, 'stopReason');
        if (stopReason !== undefined) ctx.finalAssistantStopReason = stopReason;
      }
      return;
    }

    // omp 无 auto_retry / compaction 帧；agent_end 只是帧序号收口，
    // 终态由 prompt_result 决定。
    case 'agent_end':
      return;

    case 'prompt_result': {
      if (ctx.turnSettled) return;
      settleOmpRun(ctx, event, queue);
      return;
    }

    default:
      ctx.logger.debug('omp translator: unhandled event dropped', { type: event.type });
  }
}

/** prompt_result 是 omp 的 turn 终态：completed → done.completed；aborted → cancelled；
 *  error → done.failed。 */
function settleOmpRun(
  ctx: OmpTranslateContext,
  event: PiRpcEvent,
  queue: AsyncQueue<AgentEvent>,
): void {
  ctx.turnSettled = true;
  ctx.isStreaming = false;
  const error = frameRecord(event, 'error');
  const errorText = error ? readFrameString(error, 'message') : undefined;
  const status = readFrameString(event, 'status');
  const outcome = status === 'error'
    ? 'failed'
    : status === 'aborted' || ctx.finalAssistantStopReason === 'aborted'
      ? 'cancelled'
      : 'completed';
  const usage = ompTurnUsageOf(ctx);
  if (outcome === 'failed') {
    queue.push({
      type: 'error',
      data: { message: errorText ?? 'omp run failed', sdkError: errorText, isTerminal: true, usage },
      source: 'omp',
    });
  }
  queue.push({
    type: 'done',
    data: {
      type: 'omp/agent_settled',
      status: outcome,
      usage,
    },
    source: 'omp',
  });
  // 与 Claude/Codex/pi 的 turn-end status 契约一致：isRunning=false 收口。
  pushStatus(queue, ctx, 'Done', false);
}

function ompTurnUsageOf(ctx: OmpTranslateContext) {
  return {
    inputTokens: ctx.turnInput,
    outputTokens: ctx.turnOutput,
    cacheReadTokens: ctx.turnCacheRead,
    cacheCreationTokens: ctx.turnCacheWrite,
  };
}

function ompToolResultFullText(result: unknown): string {
  const content = typeof result === 'object' && result !== null && 'content' in result
    && Array.isArray(result.content)
    ? result.content
    : [];
  const parts: string[] = [];
  for (const item of content) {
    if (typeof item !== 'object' || item === null) continue;
    const record = item as Record<string, unknown>;
    if (record.type === 'text' && typeof record.text === 'string') parts.push(record.text);
    else if (record.type === 'image') parts.push('[image]');
  }
  return parts.join('\n');
}

function handleAssistantDelta(
  delta: Record<string, unknown>,
  queue: AsyncQueue<AgentEvent>,
  ctx: OmpTranslateContext,
): void {
  const contentIndex = readFrameNumber(delta, 'contentIndex') ?? 0;

  switch (delta.type) {
    case 'text_delta': {
      const textDelta = readFrameString(delta, 'delta');
      if (textDelta && textDelta.length > 0) {
        queue.push({ type: 'text', data: { text: textDelta, isFinal: false }, source: 'omp' });
      }
      return;
    }

    case 'text_end': {
      // omp 的 message_update 不发 message_end 全文校准，text_end 即本块全文；
      // 内容跨块拼接由消费方按 isFinal 顺序累积（与 pi 的 per-message 覆盖不同）。
      const text = readFrameString(delta, 'content');
      if (text && text.length > 0) {
        ctx.finalAssistantText = text;
        queue.push({
          type: 'text',
          data: { text, isFinal: true, isFullText: true },
          source: 'omp',
        });
      }
      return;
    }

    case 'thinking_start': {
      ensureThinkingBlock(contentIndex, queue, ctx);
      return;
    }

    case 'thinking_delta': {
      const block = ensureThinkingBlock(contentIndex, queue, ctx);
      const textDelta = readFrameString(delta, 'delta');
      if (textDelta && textDelta.length > 0) {
        queue.push({
          type: 'thinking',
          data: { stage: 'delta', blockId: block.blockId, text: textDelta },
          source: 'omp',
        });
      }
      return;
    }

    case 'thinking_end': {
      const block = ensureThinkingBlock(contentIndex, queue, ctx);
      ctx.thinkingBlocks.delete(contentIndex);
      queue.push({
        type: 'thinking',
        data: {
          stage: 'final',
          blockId: block.blockId,
          text: readFrameString(delta, 'content') ?? '',
          durationMs: Date.now() - block.startedAt,
        },
        source: 'omp',
      });
      return;
    }

    // toolcall_* 由 tool_execution_* 承载；done/error 是 omp assistantMessageEvent
    // 的尾类型（终态经 prompt_result 上报），流内出现时静默。
    case 'toolcall_start':
    case 'toolcall_delta':
    case 'toolcall_end':
    case 'done':
    case 'error':
      return;

    default:
      ctx.logger.debug('omp translator: unhandled assistant delta', { type: delta.type });
  }
}

function ensureThinkingBlock(
  contentIndex: number,
  queue: AsyncQueue<AgentEvent>,
  ctx: OmpTranslateContext,
): { blockId: string; startedAt: number } {
  const existing = ctx.thinkingBlocks.get(contentIndex);
  if (existing) return existing;
  const block = {
    blockId: `omp-thinking-${++ctx.thinkingSeq}`,
    startedAt: Date.now(),
  };
  ctx.thinkingBlocks.set(contentIndex, block);
  queue.push({
    type: 'thinking',
    data: { stage: 'start', blockId: block.blockId, startedAt: block.startedAt },
    source: 'omp',
  });
  return block;
}
