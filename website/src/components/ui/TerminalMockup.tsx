import { useState, useEffect, useRef } from "react";

interface TerminalCommand {
  input: string;
  output: string;
}

interface TerminalMockupProps {
  commands: TerminalCommand[];
}

interface RenderedLine {
  type: "input" | "output";
  text: string;
}

export function TerminalMockup({ commands }: TerminalMockupProps) {
  const [lines, setLines] = useState<RenderedLine[]>([]);
  const [isAnimating, setIsAnimating] = useState(false);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const hasStarted = useRef(false);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting && !hasStarted.current) {
            hasStarted.current = true;
            observer.disconnect();
            setIsAnimating(true);
          }
        }
      },
      { threshold: 0.3 },
    );

    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    if (!isAnimating) return;

    let cancelled = false;

    async function animate() {
      for (const command of commands) {
        if (cancelled) break;

        // Type input character by character
        for (let i = 0; i <= command.input.length; i++) {
          if (cancelled) break;
          const partial = command.input.slice(0, i);
          setLines((prev) => {
            const next = [...prev];
            const lastIdx = next.length - 1;
            if (lastIdx >= 0 && next[lastIdx].type === "input") {
              next[lastIdx] = { type: "input", text: partial };
            } else {
              next.push({ type: "input", text: partial });
            }
            return next;
          });
          await sleep(40);
        }

        if (cancelled) break;
        await sleep(500);

        // Show output immediately
        if (!cancelled) {
          setLines((prev) => [
            ...prev,
            { type: "output", text: command.output },
          ]);
        }

        await sleep(300);
      }
    }

    void animate();
    return () => {
      cancelled = true;
    };
  }, [isAnimating, commands]);

  return (
    <div
      ref={containerRef}
      className="rounded-xl border border-[var(--color-border)] bg-[#0a1a24] overflow-hidden"
    >
      {/* Title bar */}
      <div className="flex items-center px-4 py-3 bg-[#081520] border-b border-[var(--color-border)] relative">
        {/* Traffic lights */}
        <div className="flex items-center gap-2 absolute left-4">
          <span className="w-3 h-3 rounded-full bg-[#ff6b5e] block" />
          <span className="w-3 h-3 rounded-full bg-[var(--color-warning)] block" />
          <span className="w-3 h-3 rounded-full bg-[var(--color-success)] block" />
        </div>
        {/* Centered title */}
        <span className="w-full text-center text-xs text-[var(--color-text-secondary)] font-mono select-none">
          EarlySalty Bot
        </span>
      </div>

      {/* Body */}
      <div className="p-5 font-mono text-sm space-y-3 min-h-[140px]">
        {lines.map((line, i) =>
          line.type === "input" ? (
            <div key={i} className="text-[var(--color-text-secondary)]">
              <span className="text-[var(--color-primary)] mr-2">{">"}</span>
              {line.text}
              {i === lines.length - 1 && (
                <span className="animate-pulse text-[var(--color-primary)] ml-0.5">
                  ▋
                </span>
              )}
            </div>
          ) : (
            <div key={i} className="text-[var(--color-accent)] pl-5">
              {line.text}
            </div>
          ),
        )}
      </div>
    </div>
  );
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
