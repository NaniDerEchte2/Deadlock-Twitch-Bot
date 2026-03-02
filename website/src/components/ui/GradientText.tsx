import type { ReactNode } from "react";

interface GradientTextProps {
  children: ReactNode;
  className?: string;
}

export function GradientText({ children, className = "" }: GradientTextProps) {
  return (
    <span
      className={`bg-clip-text text-transparent ${className}`}
      style={{
        backgroundImage: "linear-gradient(135deg, #ff7a18, #10b7ad)",
      }}
    >
      {children}
    </span>
  );
}
