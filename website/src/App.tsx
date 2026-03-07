import { Navbar } from '@/components/layout/Navbar'
import { Footer } from '@/components/layout/Footer'
import { GlowOrb } from '@/components/effects/GlowOrb'
import { Hero } from '@/components/sections/Hero'
import { Stats } from '@/components/sections/Stats'
import { Features } from '@/components/sections/Features'
import { RaidSystem } from '@/components/sections/RaidSystem'
import { Dashboard } from '@/components/sections/Dashboard'
import { ClipManager } from '@/components/sections/ClipManager'
import { Community } from '@/components/sections/Community'
import { AffiliateSection } from '@/components/sections/AffiliateSection'
import { Commands } from '@/components/sections/Commands'
import { CTA } from '@/components/sections/CTA'

export default function App() {
  return (
    <>
      <GlowOrb />
      <Navbar />
      <main>
        <Hero />
        <Stats />
        <Features />
        <RaidSystem />
        <Dashboard />
        <ClipManager />
        <Community />
        <AffiliateSection />
        <Commands />
        <CTA />
      </main>
      <Footer />
    </>
  )
}
