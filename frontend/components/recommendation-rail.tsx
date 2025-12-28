import { ProductCard } from "./product-card"

interface RecommendationRailProps {
  title: string
  description?: string
  products: any[]
}

export function RecommendationRail({ title, description, products }: RecommendationRailProps) {
  return (
    <section className="space-y-6">
      <div className="space-y-1">
        <h2 className="text-2xl font-bold tracking-tight">{title}</h2>
        {description && <p className="text-muted-foreground">{description}</p>}
      </div>
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-6">
        {products.map((product, idx) => (
          <ProductCard
            key={idx}
            id={product.id || String(idx)} // passing id to ProductCard
            title={product.title}
            category={product.category}
            rating={product.rating}
            reviews={product.reviews}
            image={product.image}
            isNew={product.isNew}
          />
        ))}
      </div>
    </section>
  )
}
