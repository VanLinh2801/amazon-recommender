import { Sparkles } from "lucide-react"

export function Footer() {
  return (
    <footer className="border-t bg-secondary/30">
      <div className="container px-4 py-12 md:px-6">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
          <div className="space-y-4">
            <div className="flex items-center gap-2">
              <Sparkles className="h-5 w-5 text-primary" />
              <span className="text-lg font-semibold">RecommendAI</span>
            </div>
            <p className="text-sm text-muted-foreground">
              A showcase for advanced product recommendation algorithms and real-time user interaction tracking.
            </p>
          </div>
          <div>
            <h3 className="text-sm font-semibold mb-4 uppercase tracking-wider">Features</h3>
            <ul className="space-y-2 text-sm text-muted-foreground">
              <li>Personalized Feed</li>
              <li>Collaborative Filtering</li>
              <li>Content-based Recommendations</li>
              <li>Interaction Analytics</li>
            </ul>
          </div>
          <div>
            <h3 className="text-sm font-semibold mb-4 uppercase tracking-wider">Project</h3>
            <ul className="space-y-2 text-sm text-muted-foreground">
              <li>About</li>
              <li>Documentation</li>
              <li>GitHub</li>
              <li>Privacy Policy</li>
            </ul>
          </div>
          <div>
            <h3 className="text-sm font-semibold mb-4 uppercase tracking-wider">Developer</h3>
            <p className="text-sm text-muted-foreground">
              Built to demonstrate recommendation engine capabilities using modern web technologies.
            </p>
          </div>
        </div>
        <div className="mt-12 pt-8 border-t text-center text-sm text-muted-foreground">
          © {new Date().getFullYear()} RecommendAI. All rights reserved.
        </div>
      </div>
    </footer>
  )
}
