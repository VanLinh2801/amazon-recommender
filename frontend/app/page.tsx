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
  const [metrics, setMetrics] = useState<any>(null)
  const [metricsLoading, setMetricsLoading] = useState(true)

  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true)

        // Load recommendations cho cả user đã đăng nhập và chưa đăng nhập
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
      } catch (error) {
        console.error("Error loading data:", error)
      } finally {
        setLoading(false)
      }
    }

    async function loadMetrics() {
      try {
        setMetricsLoading(true)
        const data = await api.getModelMetrics()
        if (data.success && data.data) {
          setMetrics(data.data)
        }
      } catch (error) {
        console.error("Failed to load metrics:", error)
      } finally {
        setMetricsLoading(false)
      }
    }

    loadData()
    loadMetrics()
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
              <div className="relative aspect-video rounded-xl overflow-hidden shadow-2xl bg-secondary/50 p-6">
                {metricsLoading ? (
                  <div className="flex items-center justify-center h-full">
                    <span className="text-muted-foreground">Loading metrics...</span>
                  </div>
                ) : metrics ? (
                  <div className="h-full flex flex-col justify-center space-y-4">
                    <h3 className="text-lg font-semibold mb-4">Model Performance Metrics</h3>
                    <div className="grid grid-cols-2 gap-4">
                      <div className="bg-background/50 rounded-lg p-3">
                        <div className="text-sm text-muted-foreground">RMSE</div>
                        <div className="text-2xl font-bold">{metrics.rmse?.toFixed(4) || 'N/A'}</div>
                      </div>
                      <div className="bg-background/50 rounded-lg p-3">
                        <div className="text-sm text-muted-foreground">MAE</div>
                        <div className="text-2xl font-bold">{metrics.mae?.toFixed(4) || 'N/A'}</div>
                      </div>
                      <div className="bg-background/50 rounded-lg p-3">
                        <div className="text-sm text-muted-foreground">Precision@10</div>
                        <div className="text-2xl font-bold">{(metrics['precision@10'] * 100)?.toFixed(2) || 'N/A'}%</div>
                      </div>
                      <div className="bg-background/50 rounded-lg p-3">
                        <div className="text-sm text-muted-foreground">Recall@10</div>
                        <div className="text-2xl font-bold">{(metrics['recall@10'] * 100)?.toFixed(2) || 'N/A'}%</div>
                      </div>
                    </div>
                    <div className="text-xs text-muted-foreground mt-2">
                      Lower RMSE/MAE is better • Higher Precision/Recall is better
                    </div>
                  </div>
                ) : (
                  <div className="flex items-center justify-center h-full">
                    <span className="text-muted-foreground">Metrics not available</span>
                  </div>
                )}
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
              {recommendations.length > 0 ? (
                <RecommendationRail
                  title={user ? "Recommended for You" : "Popular Products"}
                  description={
                    user
                      ? "Based on your recent browsing and purchase history."
                      : "Sign in to see personalized recommendations based on your preferences."
                  }
                  products={recommendations}
                  isAuthenticated={!!user}
                />
              ) : (
                <div className="text-center py-12 space-y-4">
                  {error ? (
                    <div className="space-y-2">
                      <p className="text-destructive font-medium">{error}</p>
                      <p className="text-muted-foreground text-sm">
                        {user
                          ? "Try browsing some products or adding items to your cart to get personalized recommendations."
                          : "Try refreshing the page or sign in for personalized recommendations."}
                      </p>
                    </div>
                  ) : (
                    <p className="text-muted-foreground">
                      {user
                        ? "No recommendations available yet. Start browsing to get personalized recommendations!"
                        : "No recommendations available. Sign in for personalized recommendations."}
                    </p>
                  )}
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
