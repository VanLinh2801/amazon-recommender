"use client"

import { ReactNode } from 'react'
import { CartProvider } from './cart-context'
import { useAuth } from './auth-context'

export function CartProviderWrapper({ children }: { children: ReactNode }) {
  const { user } = useAuth()
  
  return <CartProvider userId={user?.id || null}>{children}</CartProvider>
}

