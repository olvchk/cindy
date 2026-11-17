/**
 * OmpAgent 单元测试 —— fake transport 驱动,喂 tools/omp/rpc-capture-reference.jsonl
 * 里的代表性帧序,断言翻译出的 AgentEvent 流(行为断言,不测内部)。
 *
 * 覆盖:
 *  - 完整 prompt 周期:thinking → text 流 → tool_use → tool_result → done completed
 *  - abort:写出 {type:'abort'} 帧并关闭 transport
 *  - prompt_result error/aborted → failed/cancelled done
 *  - 未知帧丢弃,不崩流
 *  - usage 快照口径
 */

import { describe, expect, it, vi } from 'vitest';

import type { AgentEvent } from '../../../types/events.js';
import type { Logger } from '../../../interfaces/logger.js';
import type { PiTransport, PiLineHandler, PiCloseHandler } from '../../pi/transport.js';
import { PiRpcProcess } from '../../pi/rpc-client.js';
import { createAsyncQueue, type AsyncQueue } from '../../shared/async-queue.js';
import {
  createOmpTranslateContext,
  translateOmpEvent,
  ompUsageSnapshotOf,
} from '../translator.js';

interface FakeTransportHarness {
  transport: PiTransport & { written: string[] };
  emitLine: (line: string) => void;
  drain: () => void;
  written: string[];
}

function makeFakeTransport(): FakeTransportHarness {
  const written: string[] = [];
  let lineHandler: PiLineHandler | undefined;
  let closeHandler: PiCloseHandler | undefined;
  const { resolve: drain, promise: writeResolved } = Promise.withResolvers<void>();
  const writeLineImpl = (line: string): Promise<void> => {
    written.push(line);
    return writeResolved;
  };
  const transport: PiTransport & { written: string[] } = {
    writeLine: vi.fn(writeLineImpl),
    onLine: vi.fn((h: PiLineHandler) => { lineHandler = h; return () => { lineHandler = undefined; }; }),
    onClose: vi.fn((h: PiCloseHandler) => { closeHandler = h; return () => { closeHandler = undefined; }; }),
    onStderr: vi.fn(() => () => undefined),
    close: vi.fn(async () => { closeHandler?.({ code: 0, signal: null, reason: 'test close' }); }),
    isClosed: vi.fn(() => false),
    get pid() { return 4321; },
    written,
  };
  return {
    transport,
    emitLine: (line) => lineHandler?.(line),
    drain: () => drain(),
    written,
  };
}

interface TestLogger extends Logger {
  warn: ReturnType<typeof vi.fn>;
  debug: ReturnType<typeof vi.fn>;
}

function makeLogger(): TestLogger {
  const warn = vi.fn();
  const debug = vi.fn();
  const logger: Logger = {
    trace: vi.fn(), info: vi.fn(), warn, error: vi.fn(), fatal: vi.fn(), debug,
    child: () => makeLogger(),
  };
  return logger as TestLogger;
}

/** 从 fixture 摘出的最小代表性帧序(prompt 周期,含 thinking/text/tool/终态)。 */
const PROMPT_CYCLE_FRAMES: Array<Record<string, unknown>> = [
  { type: 'response', id: 'c1', command: 'prompt', success: true },
  { type: 'agent_start' },
  { type: 'turn_start' },
  { type: 'message_start', message: { role: 'user', content: [{ type: 'text', text: 'List the files' }] }, messageId: 'msg-1' },
  { type: 'message_update', assistantMessageEvent: { type: 'thinking_start', contentIndex: 0, partial: { role: 'assistant', content: [{ type: 'thinking', thinking: 'User' }] } } },
  { type: 'message_update', assistantMessageEvent: { type: 'thinking_delta', contentIndex: 0, delta: 'User wants a listing' } },
  { type: 'message_update', assistantMessageEvent: { type: 'thinking_end', contentIndex: 0, content: 'User wants a listing' } },
  { type: 'message_update', assistantMessageEvent: { type: 'text_start', contentIndex: 1 } },
  { type: 'message_update', assistantMessageEvent: { type: 'text_delta', contentIndex: 1, delta: 'Root of ' } },
  { type: 'message_update', assistantMessageEvent: { type: 'text_delta', contentIndex: 1, delta: 'repo' } },
  { type: 'message_update', assistantMessageEvent: { type: 'text_end', contentIndex: 1, content: 'Root of repo' } },
  { type: 'message_update', assistantMessageEvent: { type: 'toolcall_start', contentIndex: 2 } },
  { type: 'message_update', assistantMessageEvent: { type: 'toolcall_delta', contentIndex: 2, delta: '{"path": "D:/repo"}' } },
  { type: 'message_update', assistantMessageEvent: { type: 'toolcall_end', contentIndex: 2, toolCall: { type: 'toolCall', id: 't1', name: 'read', arguments: { path: 'D:/repo' } } } },
  { type: 'tool_execution_start', toolCallId: 't1', toolName: 'read', args: { path: 'D:/repo' } },
  { type: 'tool_execution_end', toolCallId: 't1', toolName: 'read', result: { content: [{ type: 'text', text: 'files...' }] }, isError: false },
  { type: 'turn_end', message: { role: 'assistant', content: [{ type: 'text', text: 'Root of repo' }], usage: { input: 24413, output: 214, cacheRead: 0, cacheWrite: 0, totalTokens: 24627, cost: { total: 0.0037 } }, stopReason: 'stop' } },
  { type: 'agent_end', messages: [] },
  { type: 'prompt_result', id: 'c1', agentInvoked: true, status: 'completed', sessionSettled: true },
  { type: 'session_settled' },
];

interface TextEventData {
  text: string;
  isFinal: boolean;
  isFullText?: boolean;
}

interface DoneEventData {
  status: string;
  usage?: { inputTokens: number; outputTokens: number };
}

interface ToolUseEventData {
  toolUseId: string;
  toolName: string;
}

interface ToolResultFullEventData {
  toolUseId: string;
  fullText: string;
  isError: boolean;
}

interface ErrorEventData {
  message: string;
  isTerminal?: boolean;
}

interface StatusEventData {
  status: string;
  isRunning: boolean;
}

/** 先挂消费者再喂帧(push 同步);collect 到哨兵条件为止。 */
function startCollect(
  queue: AsyncQueue<AgentEvent>,
  isSentinel: (event: AgentEvent, collected: AgentEvent[]) => boolean,
): Promise<AgentEvent[]> {
  const run = (async () => {
    const out: AgentEvent[] = [];
    for await (const item of queue) {
      out.push(item);
      if (isSentinel(item, out)) break;
    }
    return out;
  })();
  return run;
}

describe('omp translator (fixture-driven frames)', () => {
  it('translates a recorded prompt cycle into text/thinking/tool/done events', async () => {
    const logger = makeLogger();
    const queue = createAsyncQueue<AgentEvent>();
    const ctx = createOmpTranslateContext(logger);
    // 哨兵:done 已见 + 收到终态 Done status
    const eventsPromise = startCollect(queue, (event, out) =>
      event.type === 'status' && out.some((o) => o.type === 'done'));

    for (const frame of PROMPT_CYCLE_FRAMES) {
      translateOmpEvent(frame as never, queue, ctx);
    }

    const events = await eventsPromise;
    const types = events.map((e) => e.type);
    expect(types[0]).toBe('status'); // agent_start → Working…
    expect(types).toContain('thinking');
    expect(types).toContain('text');
    expect(types).toContain('tool_use');
    expect(types).toContain('tool_result_full');
    expect(types).toContain('tool_result');
    expect(types).toContain('done');

    const textEvents = events.filter((e) => e.type === 'text').map((e) => e.data as TextEventData);
    const deltas = textEvents.filter((e) => !e.isFinal).map((e) => e.text);
    expect(deltas.join('')).toContain('Root of');
    const finalText = textEvents.find((e) => e.isFinal);
    expect(finalText?.text).toBe('Root of repo');
    expect(finalText?.isFullText).toBe(true);

    const toolUse = events.find((e) => e.type === 'tool_use')!.data as ToolUseEventData;
    expect(toolUse.toolUseId).toBe('t1');
    expect(toolUse.toolName).toBe('read');
    const toolResultFull = events.find((e) => e.type === 'tool_result_full')!.data as ToolResultFullEventData;
    expect(toolResultFull.toolUseId).toBe('t1');
    expect(toolResultFull.fullText).toBe('files...');
    expect(toolResultFull.isError).toBe(false);

    const done = events.find((e) => e.type === 'done')!.data as DoneEventData;
    expect(done.status).toBe('completed');
    expect(done.usage?.inputTokens).toBe(24413);
    expect(done.usage?.outputTokens).toBe(214);

    const lastStatus = events.filter((e) => e.type === 'status').at(-1)!.data as StatusEventData;
    expect(lastStatus.isRunning).toBe(false);
    expect(lastStatus.status).toBe('Done');
  });

  it('drops unknown frames without crashing the stream', async () => {
    const logger = makeLogger();
    const queue = createAsyncQueue<AgentEvent>();
    const ctx = createOmpTranslateContext(logger);
    const eventsPromise = startCollect(queue, () => true);

    // omp 插件桥 / 命令目录帧:静默忽略
    translateOmpEvent({ type: 'extension_ui_request', method: 'setStatus' } as never, queue, ctx);
    translateOmpEvent({ type: 'available_commands_update', commands: [] } as never, queue, ctx);
    // 完全未知的帧:debug 记录后丢弃
    translateOmpEvent({ type: 'unknown_future_frame', body: {} } as never, queue, ctx);
    translateOmpEvent({ type: 'agent_start' } as never, queue, ctx);

    const events = await eventsPromise;
    expect(events.map((e) => e.type)).toEqual(['status']);
    expect(logger.debug).toHaveBeenCalled();
  });

  it('maps prompt_result error to failed done with a terminal error event', async () => {
    const logger = makeLogger();
    const queue = createAsyncQueue<AgentEvent>();
    const ctx = createOmpTranslateContext(logger);
    const eventsPromise = startCollect(queue, (event, out) =>
      event.type === 'status' && out.some((o) => o.type === 'done'));

    translateOmpEvent({ type: 'agent_start' } as never, queue, ctx);
    translateOmpEvent({
      type: 'prompt_result', id: 'c1', agentInvoked: true, status: 'error',
      error: { message: 'model overloaded', retryable: true },
    } as never, queue, ctx);

    const events = await eventsPromise;
    const errorEvent = events.find((e) => e.type === 'error')!.data as ErrorEventData;
    expect(errorEvent.message).toBe('model overloaded');
    expect(errorEvent.isTerminal).toBe(true);
    const done = events.find((e) => e.type === 'done')!.data as DoneEventData;
    expect(done.status).toBe('failed');
  });

  it('maps prompt_result aborted to cancelled done', async () => {
    const logger = makeLogger();
    const queue = createAsyncQueue<AgentEvent>();
    const ctx = createOmpTranslateContext(logger);
    const eventsPromise = startCollect(queue, (event, out) =>
      event.type === 'status' && out.some((o) => o.type === 'done'));

    translateOmpEvent({ type: 'agent_start' } as never, queue, ctx);
    translateOmpEvent({ type: 'prompt_result', id: 'c1', status: 'aborted', agentInvoked: true } as never, queue, ctx);

    const events = await eventsPromise;
    const done = events.find((e) => e.type === 'done')!.data as DoneEventData;
    expect(done.status).toBe('cancelled');
  });

  it('usage snapshot reports turn tokens and zero unknown contextWindow', () => {
    const logger = makeLogger();
    const queue = createAsyncQueue<AgentEvent>();
    const ctx = createOmpTranslateContext(logger);

    translateOmpEvent({
      type: 'turn_end',
      message: { role: 'assistant', usage: { input: 100, output: 20 } },
    } as never, queue, ctx);

    const snapshot = ompUsageSnapshotOf(ctx);
    expect(snapshot.tokenUsage).toBe(120);
    expect(snapshot.contextTokens).toBe(100);
    expect(snapshot.contextWindow).toBe(0);
  });
});

describe('OmpAgent rpc protocol (PiRpcProcess over fake transport)', () => {
  it('streams translated events and completes a turn over a fake transport', async () => {
    const { transport, emitLine, drain } = makeFakeTransport();
    const logger = makeLogger();
    const queue = createAsyncQueue<AgentEvent>();
    const ctx = createOmpTranslateContext(logger);
    const proc = new PiRpcProcess({
      transport,
      logger,
      onEvent: (event) => translateOmpEvent(event as never, queue, ctx),
      onExit: () => {},
    });

    const eventsPromise = startCollect(queue, (event, out) =>
      event.type === 'status' && out.some((o) => o.type === 'done'));

    const request = proc.request({ type: 'prompt', message: 'hi' }, {
      timeoutMs: 5_000,
      refreshTimeoutOnEvent: (event) => event.type !== 'response',
    });
    drain();
    const sentFrame = JSON.parse(transport.written[0]) as { id: string };
    for (const frame of PROMPT_CYCLE_FRAMES) {
      emitLine(JSON.stringify(frame.type === 'response' ? { ...frame, id: sentFrame.id } : frame));
    }
    await request;
    const events = await eventsPromise;
    expect(events.map((e) => e.type)).toContain('text');
    expect(events.map((e) => e.type)).toContain('done');
  });

  it('abort writes the abort frame and closes the transport', async () => {
    const { transport, drain } = makeFakeTransport();
    const logger = makeLogger();
    const queue = createAsyncQueue<AgentEvent>();
    const proc = new PiRpcProcess({
      transport,
      logger,
      onEvent: (event) => translateOmpEvent(event as never, queue, createOmpTranslateContext(logger)),
      onExit: () => {},
    });

    proc.send({ type: 'abort' });
    await drain();
    expect(transport.written.some((line) => {
      const frame = JSON.parse(line) as { type?: string };
      return frame.type === 'abort';
    })).toBe(true);

    await proc.close();
    expect(transport.close).toHaveBeenCalled();
  });
});
