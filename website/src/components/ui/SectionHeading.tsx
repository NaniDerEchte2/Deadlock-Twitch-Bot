import { ScrollReveal } from "./ScrollReveal";

interface SectionHeadingProps {
  title: string;
  subtitle?: string;
  badge?: string;
  className?: string;
}

export function SectionHeading({
  title,
  subtitle,
  badge,
  className = "",
}: SectionHeadingProps) {
  return (
    <ScrollReveal className={`text-center ${className}`}>
      {badge && (
        <p className="text-sm uppercase tracking-wider font-medium text-[var(--color-primary)] mb-3">
          {badge}
        </p>
      )}
      <h2 className="text-4xl md:text-5xl font-bold text-[var(--color-text-primary)] font-display">
        {title}
      </h2>
      {subtitle && (
        <p className="text-lg text-[var(--color-text-secondary)] mt-4 max-w-2xl mx-auto">
          {subtitle}
        </p>
      )}
    </ScrollReveal>
  );
}
