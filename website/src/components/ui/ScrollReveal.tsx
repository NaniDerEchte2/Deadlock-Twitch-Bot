import { motion } from "framer-motion";
import type { ReactNode } from "react";

interface ScrollRevealProps {
  children: ReactNode;
  className?: string;
  delay?: number;
  direction?: "up" | "down" | "left" | "right";
}

export function ScrollReveal({
  children,
  className,
  delay = 0,
  direction = "up",
}: ScrollRevealProps) {
  const isHorizontal = direction === "left" || direction === "right";
  const isNegative = direction === "down" || direction === "right";
  const offset = isNegative ? -30 : 30;

  const initial = isHorizontal
    ? { opacity: 0, x: offset }
    : { opacity: 0, y: offset };

  const animate = isHorizontal
    ? { opacity: 1, x: 0 }
    : { opacity: 1, y: 0 };

  return (
    <motion.div
      className={className}
      initial={initial}
      whileInView={animate}
      viewport={{ once: true, margin: "-80px" }}
      transition={{ duration: 0.6, delay, ease: "easeOut" }}
    >
      {children}
    </motion.div>
  );
}
