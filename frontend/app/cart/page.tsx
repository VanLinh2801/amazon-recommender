"use client"

import { useState } from "react"
import Link from "next/link"
import { Trash2, Plus, Minus, ArrowLeft, CreditCard, ShieldCheck, ShoppingCart } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"
import { Separator } from "@/components/ui/separator"
import { Navbar } from "@/components/navbar"
import { Footer } from "@/components/footer"
import Image from "next/image"
import { useCart } from "@/contexts/cart-context"
import { useAuth } from "@/contexts/auth-context"
import { useRouter } from "next/navigation"
import { Alert, AlertDescription } from "@/components/ui/alert"

export default function CartPage() {
  const { cart, loading, updateCartItem, removeFromCart, clearCart } = useCart()
  const { user } = useAuth()
  const router = useRouter()
  const [updating, setUpdating] = useState<string | null>(null)

  const handleUpdateQuantity = async (asin: string, newQuantity: number) => {
    if (newQuantity < 1) {
      await handleRemove(asin)
      return
    }

    setUpdating(asin)
    try {
      await updateCartItem(asin, newQuantity)
    } catch (error) {
      console.error("Failed to update cart item:", error)
    } finally {
      setUpdating(null)
    }
  }

  const handleRemove = async (asin: string) => {
    setUpdating(asin)
    try {
      await removeFromCart(asin)
    } catch (error) {
      console.error("Failed to remove item:", error)
    } finally {
      setUpdating(null)
    }
  }

  if (!user) {
    return (
      <div className="flex min-h-screen flex-col">
        <Navbar />
        <main className="flex-1 bg-secondary/10 py-12">
          <div className="container mx-auto px-4 md:px-6">
            <Alert>
              <AlertDescription>
                Please <Link href="/login" className="text-primary hover:underline">login</Link> to view your cart.
              </AlertDescription>
            </Alert>
          </div>
        </main>
        <Footer />
      </div>
    )
  }

  if (loading) {
    return (
      <div className="flex min-h-screen flex-col">
        <Navbar />
        <main className="flex-1 bg-secondary/10 py-12">
          <div className="container mx-auto px-4 md:px-6">
            <div className="flex items-center justify-center py-12">
              <p className="text-muted-foreground">Loading cart...</p>
            </div>
          </div>
        </main>
        <Footer />
      </div>
    )
  }

  const items = cart?.items || []
  const totalItems = cart?.total_items || 0

  return (
    <div className="flex min-h-screen flex-col">
      <Navbar />
      <main className="flex-1 bg-secondary/10 py-12">
        <div className="container mx-auto px-4 md:px-6">
          <div className="flex flex-col gap-8">
            <div className="flex items-center gap-2">
              <Link
                href="/"
                className="text-sm text-muted-foreground hover:text-primary flex items-center gap-1 transition-colors"
              >
                <ArrowLeft className="h-4 w-4" />
                Back to Store
              </Link>
            </div>

            {items.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-12 space-y-4">
                <ShoppingCart className="h-16 w-16 text-muted-foreground" />
                <h2 className="text-2xl font-bold">Your cart is empty</h2>
                <p className="text-muted-foreground">Start shopping to add items to your cart</p>
                <Link href="/">
                  <Button>Continue Shopping</Button>
                </Link>
              </div>
            ) : (
            <div className="grid gap-8 lg:grid-cols-3">
              <div className="lg:col-span-2 space-y-6">
                <div className="flex items-center justify-between">
                  <h1 className="text-3xl font-bold tracking-tight">Shopping Cart</h1>
                    <span className="text-muted-foreground">{totalItems} {totalItems === 1 ? "item" : "items"}</span>
                </div>

                <div className="space-y-4">
                    {items.map((item) => (
                      <Card key={item.asin} className="overflow-hidden">
                      <CardContent className="p-0">
                        <div className="flex flex-col sm:flex-row items-center gap-4 p-4">
                          <div className="relative aspect-square h-24 w-24 flex-shrink-0 overflow-hidden rounded-md border bg-muted">
                              {item.primary_image ? (
                            <Image
                                  src={item.primary_image}
                                  alt={item.title || "Product"}
                              fill
                              className="object-cover"
                            />
                              ) : (
                                <div className="flex items-center justify-center h-full text-muted-foreground">
                                  No Image
                                </div>
                              )}
                          </div>
                          <div className="flex-1 space-y-1 text-center sm:text-left">
                            <p className="text-xs text-muted-foreground font-medium uppercase tracking-wider">
                                {item.parent_asin || "Product"}
                            </p>
                              <h3 className="font-semibold text-lg line-clamp-1">
                                {item.title || `Item ${item.asin}`}
                              </h3>
                              <p className="text-muted-foreground text-sm italic">Price not available</p>
                          </div>
                          <div className="flex items-center gap-3">
                            <div className="flex items-center border rounded-md">
                                <Button
                                  variant="ghost"
                                  size="icon"
                                  className="h-8 w-8 rounded-none border-r"
                                  onClick={() => handleUpdateQuantity(item.asin, item.quantity - 1)}
                                  disabled={updating === item.asin}
                                >
                                <Minus className="h-3 w-3" />
                              </Button>
                              <span className="w-8 text-center text-sm font-medium">{item.quantity}</span>
                                <Button
                                  variant="ghost"
                                  size="icon"
                                  className="h-8 w-8 rounded-none border-l"
                                  onClick={() => handleUpdateQuantity(item.asin, item.quantity + 1)}
                                  disabled={updating === item.asin}
                                >
                                <Plus className="h-3 w-3" />
                              </Button>
                            </div>
                            <Button
                              variant="ghost"
                              size="icon"
                              className="text-muted-foreground hover:text-destructive transition-colors"
                                onClick={() => handleRemove(item.asin)}
                                disabled={updating === item.asin}
                            >
                              <Trash2 className="h-4 w-4" />
                            </Button>
                          </div>
                        </div>
                      </CardContent>
                    </Card>
                  ))}
                </div>
              </div>

              <div className="space-y-6">
                <Card>
                  <CardHeader>
                    <CardTitle>Order Summary</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span className="text-muted-foreground">Subtotal</span>
                          <span className="text-muted-foreground italic">Not available</span>
                      </div>
                      <div className="flex justify-between text-sm">
                        <span className="text-muted-foreground">Shipping</span>
                          <span className="text-muted-foreground italic">Not available</span>
                      </div>
                      <div className="flex justify-between text-sm">
                        <span className="text-muted-foreground">Tax</span>
                          <span className="text-muted-foreground italic">Not available</span>
                      </div>
                    </div>
                    <Separator />
                    <div className="flex justify-between font-bold text-lg">
                      <span>Total</span>
                        <span className="text-muted-foreground italic">Not available</span>
                    </div>
                      <p className="text-xs text-muted-foreground text-center pt-2">
                        Price information is not available in the database
                      </p>
                  </CardContent>
                  <CardFooter className="flex flex-col gap-4">
                    <Button className="w-full h-12 text-lg font-semibold gap-2">
                      <CreditCard className="h-5 w-5" />
                      Purchase Items
                    </Button>
                    <div className="flex items-center justify-center gap-2 text-xs text-muted-foreground">
                      <ShieldCheck className="h-4 w-4 text-green-600" />
                      Secure SSL Encryption
                    </div>
                  </CardFooter>
                </Card>

                <Card className="bg-primary/5 border-primary/20">
                  <CardContent className="p-4 flex items-start gap-3">
                    <div className="p-2 rounded-full bg-primary/10 text-primary">
                      <ShieldCheck className="h-5 w-5" />
                    </div>
                    <div className="space-y-1">
                      <h4 className="text-sm font-semibold">Buyer Protection</h4>
                      <p className="text-xs text-muted-foreground">
                        Your purchase is protected from checkout to delivery.
                      </p>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </div>
            )}
          </div>
        </div>
      </main>
      <Footer />
    </div>
  )
}
