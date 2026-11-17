/**
 * OmpAgent —— omp coding agent 接入（P0）。
 *
 * 形态:spawn `omp --mode rpc --no-session --approval-mode yolo`(JSONL/stdio,
 * 与 pi 同一套字节流抽象:复用 ../pi/transport.ts + ../pi/rpc-client.ts,不改 pi 文件),
 * translator.ts 把 omp 事件帧映射进统一 AgentEvent。
 *
 * 凭证/模型:omp 完全持有自己的凭证、模型、记忆与 skills(~/.omp),Cindy 不注入任何
 * 配置;deps.binaryPath 由 host 解析(env CINDY_OMP_BINARY → 默认安装路径)。
 * P0 范围:本地桌面、Full access(--approval-mode yolo)单引擎模型 'omp'。
 * 不支持:resume / steer / setModel / fork / rewind / plan mode / 权限切换
 * (能力声明 fail-closed,全部经 NotSupportedError 拒绝)。
 *
 * prompt 串行约束:omp 在当前 agent 未收口(prompt_result + agent_end)前收到第二个
 * prompt 会拒绝(success=false "Agent is already processing")。send() 在 busy 时
 * 直接抛错,Cindy UI 本就逐条发送;队列化留待 phase 2 按需加。
 */

import { randomUUID } from 'node:crypto';

import {
  BaseAgent,
  type AgentDeps,
  type AgentSessionHandle,
  type AgentSessionTeardownOptions,
  type SendOptions,
  type StartSessionOptions,
} from '../base-agent.js';
import { NotSupportedError, type Capabilities } from '../../types/capabilities.js';
import type {
  AgentEvent,
  InteractionResolver,
  UsageSnapshot,
} from '../../types/events.js';
import type { AgentKind, UserMessage } from '../../types/common.js';
import { createAsyncQueue, type AsyncQueue } from '../shared/async-queue.js';
import { createPiStdioTransport, type PiTransport } from '../pi/transport.js';
import { PiRpcProcess, type PiRpcEvent, type PiRpcSpawnOptions } from '../pi/rpc-client.js';
import {
  createOmpTranslateContext,
  disposeOmpTranslateContext,
  ompUsageSnapshotOf,
  translateOmpEvent,
  type OmpTranslateContext,
} from './translator.js';

const OMP_MODEL_DESCRIPTOR_ID = 'omp';
/** prompt 的 response 在收口前不回:以事件活跃度续期,空闲 10 分钟才算死。 */
const OMP_PROMPT_ACCEPT_TIMEOUT_MS = 10 * 60_000;

/** omp 可用权限档:仅 Full access(rpc 层以 --approval-mode yolo 启动)。 */
const OMP_PERMISSION_MODES = [
  {
    id: 'bypassPermissions' as const,
    displayName: 'Full access',
    description:
      'omp runs with full access (approval-mode yolo). Highest risk; use only for trusted tasks.',
  },
];

function ompCapabilities(): Capabilities {
  return {
    // omp 的模型由 ~/.omp 决定,Cindy 不改写;热切需要重建会话,P0 不实现。
    switchModel: {
      supported: false,
      reason: 'not-implemented',
      message: 'omp model switching requires a session rebuild (P0)',
    },
    availableModels: [
      {
        id: OMP_MODEL_DESCRIPTOR_ID,
        displayName: 'omp (inherited from ~/.omp)',
        // 0 = 未知:omp 自己的配置决定实际模型与窗口,目录不提供兜底。
        contextWindow: 0,
        efforts: [],
        defaultEffort: null,
      },
    ],
    hasFastMode: false,
    effort: { supported: false, reason: 'not-implemented' },
    effortLevels: [],
    reasoningDisplay: ['off'],
    permissionModes: OMP_PERMISSION_MODES,
    setPermissionModeMidSession: {
      supported: false,
      reason: 'not-implemented',
      message: 'omp P0 supports Full access only',
    },
    multimodal: {
      text: { supported: true },
      image: { supported: false, reason: 'not-implemented' },
      file: { supported: false, reason: 'not-implemented' },
    },
    // P0 范围线:无 fork / rewind / session tree / extra dirs。
    fork: { supported: false, reason: 'not-implemented' },
    rewind: { supported: false, reason: 'not-implemented' },
    sessionTree: { supported: false, reason: 'not-implemented' },
    abort: { supported: true },
    sameTurnSteer: {
      supported: false,
      reason: 'not-implemented',
      message: 'omp steer requires the approval bridge (phase 2)',
    },
    // omp 记忆归 ~/.omp 管,Cindy 不开关、不重置。
    memory: {
      supported: { supported: false, reason: 'not-implemented' },
    },
    extraDirs: { supported: false, reason: 'not-implemented' },
  };
}

/** busy / closed 状态下 push 终态收口,避免 UI 卡 running。 */
function pushRejectedTurn(
  queue: AsyncQueue<AgentEvent>,
  ctx: OmpTranslateContext,
  message: string,
): void {
  ctx.turnSettled = true;
  ctx.isStreaming = false;
  const usage = {
    inputTokens: ctx.turnInput,
    outputTokens: ctx.turnOutput,
    cacheReadTokens: ctx.turnCacheRead,
    cacheCreationTokens: ctx.turnCacheWrite,
  };
  queue.push({
    type: 'error',
    data: { message, isTerminal: true, usage },
    source: 'omp',
  });
  queue.push({
    type: 'done',
    data: { type: 'omp/prompt_rejected', status: 'failed', usage },
    source: 'omp',
  });
  queue.push({
    type: 'status',
    data: { status: 'Done', ...ompUsageSnapshotOf(ctx), isRunning: false },
    source: 'omp',
  });
}

export class OmpAgent extends BaseAgent {
  readonly kind: AgentKind = 'omp';
  readonly capabilities: Capabilities = ompCapabilities();

  constructor(deps: AgentDeps) {
    super(deps);
  }

  override async startSession(opts: StartSessionOptions): Promise<AgentSessionHandle> {
    if (opts.resumeSessionId) {
      throw new NotSupportedError('resume', {
        supported: false,
        reason: 'not-implemented',
        message: 'omp session resume is not wired (P0)',
      });
    }
    if (opts.planMode === true) {
      throw new NotSupportedError('planMode', {
        supported: false,
        reason: 'not-implemented',
        message: 'omp plan mode requires the approval bridge (phase 2)',
      });
    }
    if (opts.permissionMode !== undefined && opts.permissionMode !== 'bypassPermissions') {
      throw new NotSupportedError('permissionMode', {
        supported: false,
        reason: 'not-implemented',
        message: 'omp P0 supports Full access only',
      });
    }
    // opts.model 仅作目录一致性提示;omp 用 ~/.omp 自己的默认模型,Cindy 不透传。

    const transport: PiTransport = createPiStdioTransport({
      binaryPath: this.deps.binaryPath,
      args: [
        '--mode', 'rpc',
        '--no-session',
        '--cwd', opts.workingDir,
        // P0 只跑 Full access;ask/auto 的审批桥是 phase 2。
        '--approval-mode', 'yolo',
      ],
      cwd: opts.workingDir,
      env: { ...process.env } as Record<string, string | undefined>,
      logger: this.deps.logger,
    });

    const queue: AsyncQueue<AgentEvent> = createAsyncQueue<AgentEvent>();
    const ctx: OmpTranslateContext = createOmpTranslateContext(this.deps.logger);
    let closed = false;

    const rpcOpts: PiRpcSpawnOptions = {
      transport,
      logger: this.deps.logger,
      onEvent: (event: PiRpcEvent) => {
        translateOmpEvent(event, queue, ctx);
      },
      onExit: () => {
        disposeOmpTranslateContext(ctx);
        queue.end();
      },
      onDisconnect: () => {
        disposeOmpTranslateContext(ctx);
        queue.end();
      },
    };
    const proc = new PiRpcProcess(rpcOpts);

    const closeSession = async (): Promise<void> => {
      if (closed) return;
      closed = true;
      disposeOmpTranslateContext(ctx);
      try {
        await proc.close();
      } finally {
        queue.end();
      }
    };

    let activeSend: Promise<void> | null = null;
    const handle: AgentSessionHandle = {
      id: randomUUID(),
      agentKind: 'omp',
      model: OMP_MODEL_DESCRIPTOR_ID,
      async send(message: UserMessage, _opts?: SendOptions): Promise<void> {
        if (closed || proc.isClosed) {
          throw new Error('omp session is closed');
        }
        if (activeSend) {
          // omp 拒绝未收口轮内的第二个 prompt("Agent is already processing")。
          // 上层按序发送;这里 fail fast 并保留「忙」语义,不排队、不丢消息。
          throw new Error(
            'omp is still processing the previous turn; wait for prompt_result before sending again',
          );
        }
        const text = typeof message.content === 'string'
          ? message.content
          : message.content
            .filter((block) => block.type === 'text')
            .map((block) => block.text)
            .join('\n');
        const sendPromise = proc
          .request({ type: 'prompt', message: text }, {
            timeoutMs: OMP_PROMPT_ACCEPT_TIMEOUT_MS,
            refreshTimeoutOnEvent: (event) =>
              event.type !== 'response' && event.type !== 'prompt_result',
          })
          .then((resp) => {
            if (!resp.success) {
              pushRejectedTurn(queue, ctx, resp.error ?? 'omp rejected the prompt');
            }
          })
          .finally(() => {
            if (activeSend === sendPromise) activeSend = null;
          });
        activeSend = sendPromise;
        await sendPromise;
      },
      async steer(): Promise<void> {
        throw new NotSupportedError('steer', {
          supported: false,
          reason: 'not-implemented',
          message: 'omp steer requires the approval bridge (phase 2)',
        });
      },
      async abort(): Promise<void> {
        if (closed || proc.isClosed) return;
        proc.send({ type: 'abort' });
      },
      async close(_opts?: AgentSessionTeardownOptions): Promise<void> {
        await closeSession();
      },
      events(): AsyncIterable<AgentEvent> {
        return queue;
      },
      getUsageSnapshot(): UsageSnapshot {
        return ompUsageSnapshotOf(ctx);
      },
      setInteractionResolver(_resolver: InteractionResolver): void {
        // yolo 下没有审批帧;phase 2 审批桥启用时再消费 resolver。
      },
      setModel: async (): Promise<void> => {
        throw new NotSupportedError('switchModel', {
          supported: false,
          reason: 'not-implemented',
          message: 'omp model switching requires a session rebuild (P0)',
        });
      },
    };

    return handle;
  }
}
