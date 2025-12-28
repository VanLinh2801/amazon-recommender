"use client"

import { useEffect, useState } from "react"
import { useParams, useRouter } from "next/navigation"
import Image from "next/image"
import { Navbar } from "@/components/navbar"
import { Footer } from "@/components/footer"
import { RecommendationRail } from "@/components/recommendation-rail"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Star, ShoppingCart, Heart, Share2, ShieldCheck, Truck, RotateCcw } from "lucide-react"
import { api } from "@/lib/api"
import { useAuth } from "@/contexts/auth-context"
import { useCart } from "@/contexts/cart-context"

export default function ProductDetailPage() {
  const params = useParams()
  const router = useRouter()
  const { user } = useAuth()
  const { addToCart } = useCart()
  const asin = params.id as string

  const [product, setProduct] = useState<any>(null)
  const [recommendations, setRecommendations] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [addingToCart, setAddingToCart] = useState(false)

  useEffect(() => {
    async function loadProduct() {
      try {
        setLoading(true)

        // Load product details
        const productData = await api.getItem(asin)
        setProduct(productData)

        // Log view event
        if (user) {
          try {
            await api.logEvent({
              user_id: user.id,
              asin: asin,
              event_type: "view",
            })
          } catch (error) {
            console.error("Failed to log view event:", error)
          }
        }

        // Load similar items recommendations (dựa trên category)
        try {
          const recData = await api.getSimilarItems(asin, 10)
          const formattedRecs = recData.recommendations
            .filter((rec) => rec.asin !== asin) // Exclude current product
            .slice(0, 5)
            .map((rec) => ({
              id: rec.asin,
              title: rec.title,
              category: rec.main_category || "Uncategorized",
              rating: rec.avg_rating || undefined,
              reviews: rec.rating_number || undefined,
              image: rec.primary_image || "/placeholder.svg?height=400&width=400",
            }))
          setRecommendations(formattedRecs)
        } catch (error) {
          console.error("Failed to load similar items:", error)
        }
      } catch (error) {
        console.error("Error loading product:", error)
      } finally {
        setLoading(false)
      }
    }

    if (asin) {
      loadProduct()
    }
  }, [asin, user])

  const handleAddToCart = async () => {
    if (!user) {
      router.push("/login")
      return
    }

    try {
      setAddingToCart(true)

      // Add to cart
      await addToCart(asin, 1)

      // Log event
      if (user) {
        try {
          await api.logEvent({
            user_id: user.id,
            asin: asin,
            event_type: "add_to_cart",
            metadata: { quantity: 1 },
          })
        } catch (error) {
          console.error("Failed to log add_to_cart event:", error)
        }
      }
    } catch (error) {
      console.error("Failed to add to cart:", error)
    } finally {
      setAddingToCart(false)
    }
  }

  const handleClick = async () => {
    if (user) {
      try {
        await api.logEvent({
          user_id: user.id,
          asin: asin,
          event_type: "click",
        })
      } catch (error) {
        console.error("Failed to log click event:", error)
      }
    }
  }

  if (loading) {
    return (
      <div className="flex min-h-screen flex-col">
        <Navbar />
        <main className="flex-1 py-8 md:py-12">
          <div className="container mx-auto px-4 md:px-6">
            <div className="text-center py-12">
              <p className="text-muted-foreground">Loading product...</p>
            </div>
          </div>
        </main>
        <Footer />
      </div>
    )
  }

  if (!product) {
    return (
      <div className="flex min-h-screen flex-col">
        <Navbar />
        <main className="flex-1 py-8 md:py-12">
          <div className="container mx-auto px-4 md:px-6">
            <div className="text-center py-12">
              <p className="text-muted-foreground">Product not found</p>
            </div>
          </div>
        </main>
        <Footer />
      </div>
    )
  }

  return (
    <div className="flex min-h-screen flex-col">
      <Navbar />
      <main className="flex-1 py-8 md:py-12">
        <div className="container mx-auto px-4 md:px-6">
          <div className="grid gap-8 md:grid-cols-2 lg:gap-12 items-start">
            {/* Product Image */}
            <div className="relative aspect-square rounded-2xl overflow-hidden bg-secondary/30 shadow-inner">
              {product.primary_image ? (
                <Image
                  src={product.primary_image}
                  alt={product.title}
                  fill
                  className="object-cover"
                  onClick={handleClick}
                />
              ) : (
                <div className="absolute inset-0 flex items-center justify-center text-muted-foreground">
                  No Image Available
                </div>
              )}
            </div>

            {/* Product Info */}
            <div className="space-y-6">
              <div className="space-y-2">
                {product.main_category && (
                  <Badge variant="outline" className="text-primary border-primary/20">
                    {product.main_category}
                  </Badge>
                )}
                <h1 className="text-3xl font-bold tracking-tight md:text-4xl">{product.title}</h1>
                {product.avg_rating && (
                  <div className="flex items-center gap-4">
                    <div className="flex items-center gap-0.5 text-yellow-500">
                      {Array.from({ length: 5 }).map((_, i) => (
                        <Star
                          key={i}
                          className={`h-4 w-4 ${
                            i < Math.floor(product.avg_rating) ? "fill-current" : "text-muted"
                          }`}
                        />
                      ))}
                    </div>
                    <span className="text-sm text-muted-foreground">
                      {product.avg_rating.toFixed(1)} ({product.rating_number || 0} reviews)
                    </span>
                  </div>
                )}
              </div>

              <div className="text-sm text-muted-foreground italic">Price not available</div>

              <div className="flex flex-col gap-3 sm:flex-row">
                <Button
                  size="lg"
                  className="flex-1 gap-2"
                  onClick={handleAddToCart}
                  disabled={addingToCart}
                >
                  <ShoppingCart className="h-5 w-5" />
                  {addingToCart ? "Adding..." : "Add to Cart"}
                </Button>
                <div className="flex gap-3">
                  <Button size="lg" variant="outline" className="aspect-square p-0 bg-transparent">
                    <Heart className="h-5 w-5" />
                  </Button>
                  <Button size="lg" variant="outline" className="aspect-square p-0 bg-transparent">
                    <Share2 className="h-5 w-5" />
                  </Button>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4 pt-4 sm:grid-cols-3">
                <div className="flex flex-col items-center text-center gap-1 p-3 rounded-lg bg-secondary/40">
                  <Truck className="h-5 w-5 text-muted-foreground" />
                  <span className="text-xs font-medium">Free Shipping</span>
                </div>
                <div className="flex flex-col items-center text-center gap-1 p-3 rounded-lg bg-secondary/40">
                  <RotateCcw className="h-5 w-5 text-muted-foreground" />
                  <span className="text-xs font-medium">30-Day Returns</span>
                </div>
                <div className="flex flex-col items-center text-center gap-1 p-3 rounded-lg bg-secondary/40 hidden sm:flex">
                  <ShieldCheck className="h-5 w-5 text-muted-foreground" />
                  <span className="text-xs font-medium">1-Year Warranty</span>
                </div>
              </div>
            </div>
          </div>

          {recommendations.length > 0 && (
            <div className="mt-24 space-y-12">
              <RecommendationRail
                title="Because you viewed this"
                description="Our AI engine found these products might also interest you."
                products={recommendations}
              />
            </div>
          )}
        </div>
      </main>
      <Footer />
    </div>
  )
}
