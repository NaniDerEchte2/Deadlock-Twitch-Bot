import { GlowOrb } from "@/components/effects/GlowOrb";
import { AffiliateNavbar } from "@/components/layout/AffiliateNavbar";
import { Footer } from "@/components/layout/Footer";
import { AffiliateSection } from "@/components/sections/AffiliateSection";

export default function AffiliateProgramPage() {
  return (
    <>
      <GlowOrb />
      <AffiliateNavbar />
      <main>
        <AffiliateSection standalone />
      </main>
      <Footer />
    </>
  );
}
