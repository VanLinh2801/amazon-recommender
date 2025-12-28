"use client"

import { useEffect, useState } from "react"
import { Navbar } from "@/components/navbar"
import { Footer } from "@/components/footer"
import { RecommendationRail } from "@/components/recommendation-rail"
import { Button } from "@/components/ui/button"
import { api } from "@/lib/api"
import { useAuth } from "@/contexts/auth-context"

export default function Home() {
  const { user } = useAuth()
  const [recommendations, setRecommendations] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true)

        // Load recommendations if user is logged in
        if (user) {
          try {
            const recData = await api.getRecommendations(20)
            console.log("Recommendations data:", recData)
            
            if (recData && recData.recommendations) {
              const formattedRecs = recData.recommendations.map((rec) => ({
                id: rec.asin,
                title: rec.title,
                category: rec.main_category || "Uncategorized",
                rating: rec.avg_rating || undefined,
                reviews: rec.rating_number || undefined,
                image: rec.primary_image || "/placeholder.svg?height=400&width=400",
              }))
              setRecommendations(formattedRecs)
              setError(null)
              
              if (formattedRecs.length === 0) {
                setError("No recommendations available. Try browsing some products first!")
              }
            } else {
              setRecommendations([])
              setError("No recommendations data received")
            }
          } catch (error: any) {
            console.error("Failed to load recommendations:", error)
            setError(error?.message || "Failed to load recommendations")
            setRecommendations([])
          }
        } else {
          // Nếu không có user, vẫn hiển thị message
          setRecommendations([])
          setError(null)
        }
      } catch (error) {
        console.error("Error loading data:", error)
      } finally {
        setLoading(false)
      }
    }

    loadData()
  }, [user])

  return (
    <div className="flex min-h-screen flex-col">
      <Navbar />
      <main className="flex-1">
        {/* Hero Section */}
        <section className="bg-secondary/20 py-16 md:py-24">
          <div className="container mx-auto px-4 md:px-6">
            <div className="grid gap-10 lg:grid-cols-2 items-center">
              <div className="space-y-6">
                <div className="inline-block rounded-lg bg-primary/10 px-3 py-1 text-sm font-medium text-primary">
                  Powered by AI
                </div>
                <h1 className="text-4xl font-bold tracking-tighter sm:text-6xl">
                  Discover Your Next <span className="text-primary">Favorite</span> Product
                </h1>
                <p className="max-w-[600px] text-muted-foreground md:text-xl/relaxed lg:text-base/relaxed xl:text-xl/relaxed">
                  Our advanced recommendation engine learns your preferences to curate a personalized shopping
                  experience just for you.
                </p>
                <div className="flex flex-col gap-2 min-[400px]:flex-row">
                  <Button size="lg" className="px-8">
                    Start Browsing
                  </Button>
                  <Button size="lg" variant="outline" className="px-8 bg-transparent">
                    View Demo Dashboard
                  </Button>
                </div>
              </div>
              <div className="relative aspect-video rounded-xl overflow-hidden shadow-2xl bg-secondary/50 flex items-center justify-center">
                <span className="text-muted-foreground font-medium">Visualization of Recommendation Engine Data</span>
              </div>
            </div>
          </div>
        </section>

        {/* Product Sections */}
        <div className="container mx-auto px-4 py-16 md:px-6 space-y-24">
          {loading ? (
            <div className="text-center py-12">
              <p className="text-muted-foreground">Loading recommendations...</p>
            </div>
          ) : (
            <>
              {user ? (
                <>
                  {recommendations.length > 0 ? (
                    <RecommendationRail
                      title="Recommended for You"
                      description="Based on your recent browsing and purchase history."
                      products={recommendations}
                    />
                  ) : (
                    <div className="text-center py-12 space-y-4">
                      {error ? (
                        <div className="space-y-2">
                          <p className="text-destructive font-medium">{error}</p>
                          <p className="text-muted-foreground text-sm">
                            Try browsing some products or adding items to your cart to get personalized recommendations.
                          </p>
                        </div>
                      ) : (
                        <p className="text-muted-foreground">
                          No recommendations available yet. Start browsing to get personalized recommendations!
                        </p>
                      )}
                    </div>
                  )}
                </>
              ) : (
                <div className="text-center py-12 space-y-4">
                  <p className="text-muted-foreground">
                    Sign in to see personalized recommendations
                  </p>
                </div>
              )}
            </>
          )}
        </div>
      </main>
      <Footer />
    </div>
  )
}
