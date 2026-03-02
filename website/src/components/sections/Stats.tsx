import { stats } from "@/data/stats";
import { AnimatedCounter } from "@/components/ui/AnimatedCounter";
import { ScrollReveal } from "@/components/ui/ScrollReveal";

export function Stats() {
  return (
    <section id="stats" className="relative z-10">
      <div className="max-w-5xl mx-auto px-6 -mt-8 relative z-10">
        <ScrollReveal>
          <div className="panel-card rounded-2xl p-8">
            <div className="grid grid-cols-2 md:grid-cols-5 gap-8">
              {stats.map((stat) => (
                <AnimatedCounter
                  key={stat.label}
                  end={stat.value}
                  suffix={stat.suffix}
                  label={stat.label}
                />
              ))}
            </div>
          </div>
        </ScrollReveal>
      </div>
    </section>
  );
}
