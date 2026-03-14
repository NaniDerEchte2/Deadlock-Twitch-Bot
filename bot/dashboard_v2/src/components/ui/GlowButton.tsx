import type { ReactNode, MouseEventHandler } from "react";

type GlowButtonBaseProps = {
  children: ReactNode;
  variant: "primary" | "ghost";
  size?: "sm" | "md";
  className?: string;
};

type GlowButtonAsLink = GlowButtonBaseProps & {
  as?: "a";
  href: string;
  onClick?: MouseEventHandler<HTMLAnchorElement>;
};

type GlowButtonAsButton = GlowButtonBaseProps & {
  as: "button";
  href?: never;
  onClick?: MouseEventHandler<HTMLButtonElement>;
};

type GlowButtonProps = GlowButtonAsLink | GlowButtonAsButton;

export function GlowButton({
  children,
  variant,
  size = "md",
  className = "",
  ...rest
}: GlowButtonProps) {
  const sizeClasses =
    size === "sm"
      ? "px-4 py-2 text-sm rounded-lg"
      : "px-7 py-3 rounded-xl";

  const baseClasses =
    `inline-flex items-center justify-center font-semibold transition-all duration-200 cursor-pointer no-underline ${sizeClasses}`;

  const variantClasses =
    variant === "primary"
      ? "gradient-accent text-white shadow-[0_0_0_0_rgba(255,122,24,0)] hover:shadow-[0_0_24px_4px_rgba(255,122,24,0.35)] hover:brightness-110"
      : "bg-transparent border border-[var(--color-border)] text-[var(--color-text-primary)] hover:border-[var(--color-border-hover)] hover:bg-white/5";

  const combinedClasses = `${baseClasses} ${variantClasses} ${className}`;

  if (rest.as === "button") {
    return (
      <button
        onClick={rest.onClick}
        className={combinedClasses}
      >
        {children}
      </button>
    );
  }

  return (
    <a
      href={(rest as GlowButtonAsLink).href}
      onClick={(rest as GlowButtonAsLink).onClick}
      className={combinedClasses}
    >
      {children}
    </a>
  );
}
