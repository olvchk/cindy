/**
 * OmpMark —— omp 的身份 mark。
 *
 * omp 没有官方品牌 glyph,这里用 `>_` 终端提示符字形(与 ClaudeMark 像素脸 /
 * CodexMark `>_` 花形 / PiMark π 对齐的视觉重量)。variant="mono"(默认)。
 */

interface OmpMarkProps {
  size?: number;
  className?: string;
  variant?: 'mono' | 'brand';
}

export function OmpMark({ size = 14, className }: OmpMarkProps) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 24 24"
      xmlns="http://www.w3.org/2000/svg"
      className={className}
      aria-hidden
      data-ink-icon=""
    >
      <g
        fill="none"
        stroke="currentColor"
        strokeWidth={2.4}
        strokeLinecap="round"
        strokeLinejoin="round"
      >
        {/* `>` 提示符 */}
        <path d="M4.5 6.5 9.5 12l-5 5.5" />
        {/* 下划线光标 */}
        <path d="M13 17.5h7" />
      </g>
    </svg>
  );
}
