import { motion } from "framer-motion";
import type { Transition } from "framer-motion";
import type { ReactNode } from "react";

interface FadeInProps {
  children: ReactNode;
  className?: string;
  delay?: number;
  direction?: "up" | "down" | "left" | "right";
  /** Use whileInView instead of animate — useful for scroll-triggered reveals */
  onScroll?: boolean;
  /** Stagger child animations by this many seconds each */
  stagger?: number;
}

export function FadeIn({
  children,
  className,
  delay = 0,
  direction = "up",
  onScroll = false,
  stagger,
}: FadeInProps) {
  const isHorizontal = direction === "left" || direction === "right";
  const isNegative = direction === "down" || direction === "right";
  const offset = isNegative ? -20 : 20;

  const initial = isHorizontal
    ? { opacity: 0, x: offset }
    : { opacity: 0, y: offset };

  const visible = isHorizontal
    ? { opacity: 1, x: 0 }
    : { opacity: 1, y: 0 };

  const transition: Transition = stagger
    ? { duration: 0.4, delay, ease: "easeOut" as const, staggerChildren: stagger }
    : { duration: 0.4, delay, ease: "easeOut" as const };

  if (onScroll) {
    return (
      <motion.div
        className={className}
        initial={initial}
        whileInView={visible}
        viewport={{ once: true, margin: "-60px" }}
        transition={transition}
      >
        {children}
      </motion.div>
    );
  }

  return (
    <motion.div
      className={className}
      initial={initial}
      animate={visible}
      transition={transition}
    >
      {children}
    </motion.div>
  );
}
