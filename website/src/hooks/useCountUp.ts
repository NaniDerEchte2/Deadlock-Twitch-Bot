import { useState, useEffect, useRef } from "react";
import type { RefObject } from "react";

function easeOutQuart(t: number): number {
  return 1 - Math.pow(1 - t, 4);
}

export interface UseCountUpResult {
  count: number;
  ref: RefObject<HTMLElement | null>;
}

/**
 * Animated counter that starts when the attached element enters the viewport.
 * @param end      - The target number to count up to.
 * @param duration - Animation duration in milliseconds (default: 2000).
 * @returns An object with the current `count` value and a `ref` to attach to the trigger element.
 */
export function useCountUp(end: number, duration = 2000): UseCountUpResult {
  const [count, setCount] = useState(0);
  const ref = useRef<HTMLElement | null>(null);
  const hasStarted = useRef(false);

  useEffect(() => {
    const element = ref.current;
    if (!element) return;

    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting && !hasStarted.current) {
            hasStarted.current = true;
            observer.disconnect();

            const startTime = performance.now();

            function tick(now: number) {
              const elapsed = now - startTime;
              const progress = Math.min(elapsed / duration, 1);
              const easedProgress = easeOutQuart(progress);
              const currentValue = Math.round(easedProgress * end);

              setCount(currentValue);

              if (progress < 1) {
                requestAnimationFrame(tick);
              }
            }

            requestAnimationFrame(tick);
          }
        }
      },
      { threshold: 0.2 },
    );

    observer.observe(element);

    return () => {
      observer.disconnect();
    };
  }, [end, duration]);

  return { count, ref };
}
