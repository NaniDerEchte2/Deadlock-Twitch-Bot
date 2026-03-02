import { useState, useEffect } from "react";

/**
 * Tracks which section is currently in the viewport.
 * @param sectionIds - Array of element IDs to observe.
 * @param rootMargin - IntersectionObserver rootMargin (default: "-40% 0px -55% 0px").
 * @returns The ID of the currently active section, or null if none is visible.
 */
export function useScrollSpy(
  sectionIds: string[],
  rootMargin = "-40% 0px -55% 0px",
): string | null {
  const [activeId, setActiveId] = useState<string | null>(null);

  useEffect(() => {
    if (sectionIds.length === 0) return;

    const observer = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (entry.isIntersecting) {
            setActiveId(entry.target.id);
          }
        }
      },
      { rootMargin },
    );

    const elements: Element[] = [];
    for (const id of sectionIds) {
      const el = document.getElementById(id);
      if (el) {
        observer.observe(el);
        elements.push(el);
      }
    }

    return () => {
      for (const el of elements) {
        observer.unobserve(el);
      }
      observer.disconnect();
    };
  }, [sectionIds, rootMargin]);

  return activeId;
}
