"use client"

import { createContext, useContext, useEffect, useState, ReactNode } from 'react'
import { api } from '@/lib/api'

interface CartItem {
  asin: string
  quantity: number
  title: string | null
  parent_asin: string | null
  primary_image: string | null
  price: number | null
}

interface Cart {
  cart_id: number
  user_id: number
  status: string
  created_at: string
  items: CartItem[]
  total_items: number
}

interface CartContextType {
  cart: Cart | null
  loading: boolean
  refreshCart: () => Promise<void>
  addToCart: (asin: string, quantity: number) => Promise<void>
  updateCartItem: (asin: string, quantity: number) => Promise<void>
  removeFromCart: (asin: string) => Promise<void>
  clearCart: () => Promise<void>
}

const CartContext = createContext<CartContextType | undefined>(undefined)

interface CartProviderProps {
  children: ReactNode
  userId?: number | null
}

export function CartProvider({ children, userId }: CartProviderProps) {
  const [cart, setCart] = useState<Cart | null>(null)
  const [loading, setLoading] = useState(false)

  // Load cart when user is logged in
  useEffect(() => {
    if (userId) {
      refreshCart()
    } else {
      setCart(null)
    }
  }, [userId])

  const refreshCart = async () => {
    if (!userId) return

    setLoading(true)
    try {
      const cartData = await api.getCart()
      setCart(cartData)
    } catch (error) {
      console.error('Failed to load cart:', error)
      setCart(null)
    } finally {
      setLoading(false)
    }
  }

  const addToCart = async (asin: string, quantity: number) => {
    if (!userId) {
      throw new Error('Please login to add items to cart')
    }

    try {
      const updatedCart = await api.addToCart({ asin, quantity })
      setCart(updatedCart)
    } catch (error) {
      throw error
    }
  }

  const updateCartItem = async (asin: string, quantity: number) => {
    if (!userId) {
      throw new Error('Please login to update cart')
    }

    try {
      const updatedCart = await api.updateCartItem(asin, { quantity })
      setCart(updatedCart)
    } catch (error) {
      throw error
    }
  }

  const removeFromCart = async (asin: string) => {
    if (!userId) {
      throw new Error('Please login to remove items from cart')
    }

    try {
      const updatedCart = await api.removeFromCart(asin)
      setCart(updatedCart)
    } catch (error) {
      throw error
    }
  }

  const clearCart = async () => {
    if (!userId) {
      throw new Error('Please login to clear cart')
    }

    try {
      const updatedCart = await api.clearCart()
      setCart(updatedCart)
    } catch (error) {
      throw error
    }
  }

  return (
    <CartContext.Provider
      value={{
        cart,
        loading,
        refreshCart,
        addToCart,
        updateCartItem,
        removeFromCart,
        clearCart,
      }}
    >
      {children}
    </CartContext.Provider>
  )
}

export function useCart() {
  const context = useContext(CartContext)
  if (context === undefined) {
    throw new Error('useCart must be used within a CartProvider')
  }
  return context
}


