"use client"

import { useEffect } from "react"
import Image from "next/image"
import Link from "next/link"
import { useRouter } from "next/navigation"
import { Star, ShoppingCart } from "lucide-react"
import { Card, CardContent, CardFooter } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { useToast } from "@/hooks/use-toast"
import { useAuth } from "@/contexts/auth-context"
import { api } from "@/lib/api"

interface ProductCardProps {
  id: string
  title: string
  category: string
  rating?: number
  reviews?: number
  image: string
  price?: string | null
  isNew?: boolean
  isAuthenticated?: boolean
}

export function ProductCard({
  id,
  title,
  category,
  rating,
  reviews,
  image,
  price,
  isNew,
  isAuthenticated = false,
}: ProductCardProps) {
  const router = useRouter()
  const { toast } = useToast()
  const { user } = useAuth()
  
  // Log view event when component mounts (realtime tracking)
  useEffect(() => {
    if (user && id && isAuthenticated) {
      // Log view event asynchronously (don't block rendering)
      api.logEvent({
        user_id: user.id,
        asin: id,
        event_type: "view",
        metadata: { source: "recommendation", category: category }
      }).catch(error => {
        console.error("Failed to log view event:", error)
      })
    }
  }, [user, id, isAuthenticated, category])

  const handleAddToCart = (e: React.MouseEvent) => {
    e.preventDefault()
    
    if (!isAuthenticated) {
      toast({
        title: "Sign in required",
        description: "Please sign in to add items to your cart.",
        variant: "default",
      })
      router.push("/login")
      return
    }
    
    // TODO: Implement add to cart functionality
    toast({
      title: "Added to cart",
      description: `${title} has been added to your cart.`,
    })
  }

  return (
    <Card className="group overflow-hidden transition-all hover:shadow-lg">
      <Link href={`/product/${id}`}>
        <CardContent className="p-0">
          <div className="relative aspect-square overflow-hidden bg-muted">
            <Image
              src={image || "/placeholder.svg?height=400&width=400&query=product"}
              alt={title}
              fill
              className="object-cover transition-transform group-hover:scale-105"
            />
            {isNew && <Badge className="absolute top-2 right-2">New</Badge>}
          </div>
          <div className="p-4 space-y-1.5">
            <p className="text-xs text-muted-foreground font-medium uppercase tracking-wider">
              {category || "Uncategorized"}
            </p>
            <h3 className="font-semibold line-clamp-2 group-hover:text-primary transition-colors">{title}</h3>
            {(rating !== undefined || reviews !== undefined) && (
            <div className="flex items-center gap-1">
                {rating !== undefined && (
              <div className="flex items-center text-yellow-500">
                {Array.from({ length: 5 }).map((_, i) => (
                  <Star
                    key={i}
                    className={`h-3 w-3 ${i < Math.floor(rating) ? "fill-current" : "text-muted stroke-muted-foreground"}`}
                  />
                ))}
              </div>
                )}
                {reviews !== undefined && (
              <span className="text-xs text-muted-foreground">({reviews})</span>
                )}
            </div>
            )}
          </div>
        </CardContent>
      </Link>
      <CardFooter className="p-4 pt-0 flex items-center justify-between">
        {price ? (
        <span className="font-bold">{price}</span>
        ) : (
          <span className="text-sm text-muted-foreground italic">Price not available</span>
        )}
        <Button 
          size="sm" 
          variant="secondary" 
          className="h-8 w-8 p-0" 
          onClick={handleAddToCart}
          disabled={!isAuthenticated}
          title={!isAuthenticated ? "Sign in to add to cart" : "Add to cart"}
        >
          <ShoppingCart className="h-4 w-4" />
          <span className="sr-only">Add to cart</span>
        </Button>
      </CardFooter>
    </Card>
  )
}
