/**
 * omp agent 的 desktop host 装配 —— 二进制解析 / 最小 AgentDeps / 构造,
 * 集中在本模块,maker-host/index.ts 只做一次 buildOmpAgent() 调用。
 *
 * omp 完全持有自己的凭证、模型、记忆与 skills(~/.omp),Cindy 不注入任何配置:
 * 无订阅账号包装、无 models.json 生成、无 compat proxy。二进制缺失时
 * buildOmpAgent 返回 null,本次不注册 omp,对 Cindy 启动零影响。
 */

import { existsSync } from 'node:fs';
import path from 'node:path';

import { OmpAgent, type AgentDeps } from '@cindy/maker-core';
import type { AgentRuntimeConfig, AuthAdapter, AuthState } from '@cindy/maker-core';

import { createLogger } from '../logger.js';

const log = createLogger('omp-host');

const OMP_DEFAULT_INSTALL_PATH = 'C:\\Users\\olvchk\\AppData\\Local\\omp\\omp.exe';

/**
 * 解析顺序:env CINDY_OMP_BINARY → 默认安装路径 → PATH 扫描(omp.exe)。
 * 都不可用时返回 null(调用方跳过注册)。
 */
export function resolveOmpBinaryPath(): string | null {
  const env = process.env.CINDY_OMP_BINARY;
  if (env && existsSync(env)) return env;

  if (existsSync(OMP_DEFAULT_INSTALL_PATH)) return OMP_DEFAULT_INSTALL_PATH;

  for (const dir of (process.env.PATH ?? '').split(path.delimiter)) {
    if (!dir) continue;
    const candidate = path.join(dir, 'omp.exe');
    if (existsSync(candidate)) return candidate;
  }
  return null;
}

/** omp 静态鉴权态:凭证归 ~/.omp,Cindy 不参与登录/登出/凭证注入。 */
const ompAuthAdapter: AuthAdapter = {
  getState(): Promise<AuthState> {
    return Promise.resolve({ authenticated: true, identity: 'local-omp' });
  },
  triggerLogin(): Promise<AuthState> {
    return Promise.resolve({ authenticated: true, identity: 'local-omp' });
  },
  logout(): Promise<void> {
    return Promise.resolve();
  },
  getAuthEnv(): Promise<Record<string, string>> {
    return Promise.resolve({});
  },
};

/** omp 不消费 endpoint/proxy/ripgrep 等 host 资源;只给必需的默认值。 */
const ompRuntimeConfig: AgentRuntimeConfig = {
  memoryEnabled: false,
};

export function buildOmpAgent(deps: { logger: AgentDeps['logger'] }): OmpAgent | null {
  const binaryPath = resolveOmpBinaryPath();
  if (!binaryPath) {
    log.warn('omp binary unavailable; omp agent disabled for this launch');
    return null;
  }
  log.info('omp agent enabled', { binaryPath });
  return new OmpAgent({
    binaryPath,
    auth: ompAuthAdapter,
    runtimeConfig: ompRuntimeConfig,
    logger: deps.logger,
  });
}
