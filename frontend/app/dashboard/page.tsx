'use client'

import { useEffect, useState } from "react"
import { Navbar } from "@/components/navbar"
import { Footer } from "@/components/footer"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { ChartContainer, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart"
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, LineChart, Line, PieChart, Pie, Cell, AreaChart, Area, Legend, ResponsiveContainer } from "recharts"
import { Database, FileText, TrendingUp, Layers, BarChart3, Activity, Sparkles, Loader2 } from "lucide-react"
import { api } from "@/lib/api"

const COLORS = ['#3b82f6', '#8b5cf6', '#ec4899', '#f59e0b', '#10b981', '#ef4444', '#06b6d4', '#84cc16']

const chartConfig = {
  count: {
    label: "Số lượng",
    color: "hsl(var(--chart-1))",
  },
  rating: {
    label: "Rating",
    color: "hsl(var(--chart-2))",
  },
  category: {
    label: "Category",
    color: "hsl(var(--chart-3))",
  },
}

export default function DashboardPage() {
  const [loading, setLoading] = useState(true)
  const [ratingData, setRatingData] = useState<Array<{ rating: number; count: number }>>([])
  const [categoryData, setCategoryData] = useState<Array<{ category: string; count: number }>>([])
  const [topItems, setTopItems] = useState<Array<{ item_id: string; count: number }>>([])
  const [interactionStats, setInteractionStats] = useState<any>(null)
  const [userActivity, setUserActivity] = useState<any>(null)
  const [itemPopularity, setItemPopularity] = useState<Array<{ item_id: string; interaction_count: number; mean_rating: number | null }>>([])
  const [cleaningStats, setCleaningStats] = useState<any>(null)
  const [embeddingStats, setEmbeddingStats] = useState<any>(null)

  useEffect(() => {
    async function fetchData() {
      try {
        setLoading(true)
        const [
          ratingRes,
          categoryRes,
          topItemsRes,
          interactionRes,
          userActivityRes,
          popularityRes,
          cleaningRes,
          embeddingRes
        ] = await Promise.all([
          api.getRatingDistribution(),
          api.getCategoryDistribution(20),
          api.getTopItems(20),
          api.getInteractionStats(),
          api.getUserActivity(),
          api.getItemPopularity(20),
          api.getCleaningStats(),
          api.getEmbeddingStats()
        ])

        if (ratingRes.success) setRatingData(ratingRes.data)
        if (categoryRes.success) setCategoryData(categoryRes.data)
        if (topItemsRes.success) setTopItems(topItemsRes.data)
        if (interactionRes.success) setInteractionStats(interactionRes.data)
        if (userActivityRes.success) setUserActivity(userActivityRes.data)
        if (popularityRes.success) setItemPopularity(popularityRes.data)
        if (cleaningRes.success) setCleaningStats(cleaningRes.data)
        if (embeddingRes.success) setEmbeddingStats(embeddingRes.data)
      } catch (error) {
        console.error("Error fetching analytics data:", error)
      } finally {
        setLoading(false)
      }
    }

    fetchData()
  }, [])

  if (loading) {
    return (
      <div className="flex min-h-screen flex-col">
        <Navbar />
        <main className="flex-1 container px-4 py-8 md:px-6 flex items-center justify-center">
          <div className="flex flex-col items-center gap-4">
            <Loader2 className="h-8 w-8 animate-spin text-primary" />
            <p className="text-muted-foreground">Đang tải dữ liệu...</p>
          </div>
        </main>
        <Footer />
      </div>
    )
  }

  return (
    <div className="flex min-h-screen flex-col">
      <Navbar />
      <main className="flex-1 container px-4 py-8 md:px-6">
        <div className="flex flex-col gap-8">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">Data Analytics Dashboard</h1>
            <p className="text-muted-foreground">
              Phân tích chi tiết dữ liệu từ hệ thống recommendation
            </p>
          </div>


          <Tabs defaultValue="overview" className="space-y-4">
            <TabsList className="grid w-full grid-cols-5">
              <TabsTrigger value="overview">Overview</TabsTrigger>
              <TabsTrigger value="ratings">Ratings</TabsTrigger>
              <TabsTrigger value="categories">Categories</TabsTrigger>
              <TabsTrigger value="items">Top Items</TabsTrigger>
              <TabsTrigger value="cleaning">Data Cleaning</TabsTrigger>
            </TabsList>

            {/* Overview Tab */}
            <TabsContent value="overview" className="space-y-6">
              <div className="grid gap-6 md:grid-cols-2">
                {/* Rating Distribution */}
                {ratingData.length > 0 && (
                  <Card>
                    <CardHeader>
                      <CardTitle>Phân bố Rating</CardTitle>
                      <CardDescription>Phân bố số lượng ratings theo mức độ</CardDescription>
                    </CardHeader>
                    <CardContent className="p-6">
                      <div className="w-full" style={{ height: '350px' }}>
                        <ChartContainer config={chartConfig} className="w-full h-full">
                          <BarChart data={ratingData}>
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="rating" />
                            <YAxis />
                            <ChartTooltip content={<ChartTooltipContent />} />
                            <Bar dataKey="count" fill="#3b82f6" radius={[8, 8, 0, 0]} />
                          </BarChart>
                        </ChartContainer>
                      </div>
                    </CardContent>
                  </Card>
                )}

                {/* User Activity Histogram */}
                {userActivity?.histogram && (
                  <Card>
                    <CardHeader>
                      <CardTitle>Phân bố Hoạt động User</CardTitle>
                      <CardDescription>Histogram số interactions per user</CardDescription>
                    </CardHeader>
                    <CardContent className="p-6">
                      <div className="w-full" style={{ height: '350px' }}>
                        <ChartContainer config={chartConfig} className="w-full h-full">
                          <BarChart data={userActivity.histogram}>
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="range" />
                            <YAxis />
                            <ChartTooltip content={<ChartTooltipContent />} />
                            <Bar dataKey="count" fill="#8b5cf6" radius={[8, 8, 0, 0]} />
                          </BarChart>
                        </ChartContainer>
                      </div>
                    </CardContent>
                  </Card>
                )}

                {/* Train/Test Split */}
                {interactionStats?.train_count && (
                  <Card>
                    <CardHeader>
                      <CardTitle>Train/Test Split</CardTitle>
                      <CardDescription>Phân chia dữ liệu train và test</CardDescription>
                    </CardHeader>
                    <CardContent className="p-6">
                      <div className="w-full" style={{ height: '350px' }}>
                        <ChartContainer config={chartConfig} className="w-full h-full">
                          <PieChart>
                            <Pie
                              data={[
                                { name: "Train", value: interactionStats.train_count },
                                { name: "Test", value: interactionStats.test_count },
                              ]}
                              cx="50%"
                              cy="50%"
                              labelLine={false}
                              label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`}
                              outerRadius={100}
                              fill="#8884d8"
                              dataKey="value"
                            >
                              <Cell fill="#3b82f6" />
                              <Cell fill="#8b5cf6" />
                            </Pie>
                            <ChartTooltip content={<ChartTooltipContent />} />
                          </PieChart>
                        </ChartContainer>
                      </div>
                    </CardContent>
                  </Card>
                )}

                {/* Interaction Stats */}
                {interactionStats && (
                  <Card>
                    <CardHeader>
                      <CardTitle>Thống kê Interactions</CardTitle>
                      <CardDescription>Thông tin tổng quan về interactions</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <div className="text-sm text-muted-foreground">Total Interactions</div>
                          <div className="text-2xl font-bold">{interactionStats.total_interactions?.toLocaleString()}</div>
                        </div>
                        <div>
                          <div className="text-sm text-muted-foreground">Unique Users</div>
                          <div className="text-2xl font-bold">{interactionStats.unique_users?.toLocaleString()}</div>
                        </div>
                        <div>
                          <div className="text-sm text-muted-foreground">Unique Items</div>
                          <div className="text-2xl font-bold">{interactionStats.unique_items?.toLocaleString()}</div>
                        </div>
                        <div>
                          <div className="text-sm text-muted-foreground">Avg Rating</div>
                          <div className="text-2xl font-bold">{interactionStats.avg_rating?.toFixed(2)}</div>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                )}
              </div>
            </TabsContent>

            {/* Ratings Tab */}
            <TabsContent value="ratings" className="space-y-6">
              <div className="grid gap-6 md:grid-cols-2">
                {/* Rating Distribution Bar Chart */}
                {ratingData.length > 0 && (
                  <Card>
                    <CardHeader>
                      <CardTitle>Phân bố Rating (Bar Chart)</CardTitle>
                      <CardDescription>Số lượng ratings theo từng mức độ</CardDescription>
                    </CardHeader>
                    <CardContent className="p-6">
                      <div className="w-full" style={{ height: '400px' }}>
                        <ChartContainer config={chartConfig} className="w-full h-full">
                          <BarChart data={ratingData}>
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="rating" />
                            <YAxis />
                            <ChartTooltip content={<ChartTooltipContent />} />
                            <Bar dataKey="count" fill="#3b82f6" radius={[8, 8, 0, 0]}>
                              {ratingData.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                              ))}
                            </Bar>
                          </BarChart>
                        </ChartContainer>
                      </div>
                    </CardContent>
                  </Card>
                )}

                {/* Rating Distribution Pie Chart */}
                {ratingData.length > 0 && (
                  <Card>
                    <CardHeader>
                      <CardTitle>Phân bố Rating (Pie Chart)</CardTitle>
                      <CardDescription>Tỷ lệ phần trăm của từng mức rating</CardDescription>
                    </CardHeader>
                    <CardContent className="p-6">
                      <div className="w-full" style={{ height: '400px' }}>
                        <ChartContainer config={chartConfig} className="w-full h-full">
                          <PieChart>
                            <Pie
                              data={ratingData}
                              cx="50%"
                              cy="50%"
                              labelLine={false}
                              label={({ rating, percent }) => `Rating ${rating}: ${(percent * 100).toFixed(1)}%`}
                              outerRadius={120}
                              fill="#8884d8"
                              dataKey="count"
                            >
                              {ratingData.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                              ))}
                            </Pie>
                            <ChartTooltip content={<ChartTooltipContent />} />
                          </PieChart>
                        </ChartContainer>
                      </div>
                    </CardContent>
                  </Card>
                )}
              </div>
            </TabsContent>

            {/* Categories Tab */}
            <TabsContent value="categories" className="space-y-6">
              <div className="grid gap-6 md:grid-cols-2">
                {/* Category Distribution */}
                {categoryData.length > 0 && (
                  <Card>
                    <CardHeader>
                      <CardTitle>Tần suất Nhóm Sản phẩm (Top 20)</CardTitle>
                      <CardDescription>Số lượng items theo category</CardDescription>
                    </CardHeader>
                    <CardContent className="p-6">
                      <div className="w-full" style={{ height: '500px' }}>
                        <ChartContainer config={chartConfig} className="w-full h-full">
                          <BarChart data={categoryData} layout="vertical">
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis type="number" />
                            <YAxis dataKey="category" type="category" width={150} />
                            <ChartTooltip content={<ChartTooltipContent />} />
                            <Bar dataKey="count" fill="#8b5cf6" radius={[0, 8, 8, 0]}>
                              {categoryData.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                              ))}
                            </Bar>
                          </BarChart>
                        </ChartContainer>
                      </div>
                    </CardContent>
                  </Card>
                )}

                {/* Category Distribution Horizontal */}
                {categoryData.length > 0 && (
                  <Card>
                    <CardHeader>
                      <CardTitle>Tần suất Nhóm Sản phẩm (Bar Chart)</CardTitle>
                      <CardDescription>Số lượng items theo category</CardDescription>
                    </CardHeader>
                    <CardContent className="p-6">
                      <div className="w-full" style={{ height: '500px' }}>
                        <ChartContainer config={chartConfig} className="w-full h-full">
                          <BarChart data={categoryData}>
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis 
                              dataKey="category" 
                              angle={-45}
                              textAnchor="end"
                              height={120}
                              fontSize={12}
                            />
                            <YAxis />
                            <ChartTooltip content={<ChartTooltipContent />} />
                            <Bar dataKey="count" fill="#ec4899" radius={[8, 8, 0, 0]}>
                              {categoryData.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                              ))}
                            </Bar>
                          </BarChart>
                        </ChartContainer>
                      </div>
                    </CardContent>
                  </Card>
                )}
              </div>
            </TabsContent>

            {/* Top Items Tab */}
            <TabsContent value="items" className="space-y-6">
              <div className="grid gap-6 md:grid-cols-2">
                {/* Top Items by Interactions */}
                {topItems.length > 0 && (
                  <Card>
                    <CardHeader>
                      <CardTitle>Top Items theo Interactions</CardTitle>
                      <CardDescription>Top 20 items có nhiều interactions nhất</CardDescription>
                    </CardHeader>
                    <CardContent className="p-6">
                      <div className="w-full" style={{ height: '500px' }}>
                        <ChartContainer config={chartConfig} className="w-full h-full">
                          <BarChart data={topItems} layout="vertical">
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis type="number" />
                            <YAxis dataKey="item_id" type="category" width={120} fontSize={10} />
                            <ChartTooltip content={<ChartTooltipContent />} />
                            <Bar dataKey="count" fill="#10b981" radius={[0, 8, 8, 0]} />
                          </BarChart>
                        </ChartContainer>
                      </div>
                    </CardContent>
                  </Card>
                )}

                {/* Top Items by Popularity */}
                {itemPopularity.length > 0 && (
                  <Card>
                    <CardHeader>
                      <CardTitle>Top Items theo Popularity</CardTitle>
                      <CardDescription>Top 20 items có interaction_count cao nhất</CardDescription>
                    </CardHeader>
                    <CardContent className="p-6">
                      <div className="w-full" style={{ height: '500px' }}>
                        <ChartContainer config={chartConfig} className="w-full h-full">
                          <BarChart data={itemPopularity} layout="vertical">
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis type="number" />
                            <YAxis dataKey="item_id" type="category" width={120} fontSize={10} />
                            <ChartTooltip content={<ChartTooltipContent />} />
                            <Bar dataKey="interaction_count" fill="#f59e0b" radius={[0, 8, 8, 0]} />
                          </BarChart>
                        </ChartContainer>
                      </div>
                    </CardContent>
                  </Card>
                )}

                {/* Item Popularity vs Rating */}
                {itemPopularity.length > 0 && (
                  <Card>
                    <CardHeader>
                      <CardTitle>Popularity vs Rating</CardTitle>
                      <CardDescription>Mối quan hệ giữa interaction_count và mean_rating</CardDescription>
                    </CardHeader>
                    <CardContent className="p-6">
                      <div className="w-full" style={{ height: '400px' }}>
                        <ChartContainer config={chartConfig} className="w-full h-full">
                          <AreaChart data={itemPopularity}>
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="interaction_count" />
                            <YAxis yAxisId="left" />
                            <YAxis yAxisId="right" orientation="right" />
                            <ChartTooltip content={<ChartTooltipContent />} />
                            <Area 
                              yAxisId="left"
                              type="monotone" 
                              dataKey="interaction_count" 
                              stroke="#3b82f6" 
                              fill="#3b82f6" 
                              fillOpacity={0.6}
                            />
                            <Area 
                              yAxisId="right"
                              type="monotone" 
                              dataKey="mean_rating" 
                              stroke="#8b5cf6" 
                              fill="#8b5cf6" 
                              fillOpacity={0.6}
                            />
                            <Legend />
                          </AreaChart>
                        </ChartContainer>
                      </div>
                    </CardContent>
                  </Card>
                )}

                {/* Top Items Table */}
                {topItems.length > 0 && (
              <Card>
                <CardHeader>
                      <CardTitle>Top Items Chi tiết</CardTitle>
                      <CardDescription>Danh sách top items với số liệu</CardDescription>
                </CardHeader>
                <CardContent>
                      <div className="overflow-auto max-h-[400px]">
                        <table className="w-full text-sm">
                          <thead className="sticky top-0 bg-background">
                            <tr className="border-b">
                              <th className="text-left p-2">Rank</th>
                              <th className="text-left p-2">Item ID</th>
                              <th className="text-right p-2">Interactions</th>
                        </tr>
                      </thead>
                          <tbody>
                            {topItems.map((item, idx) => (
                              <tr key={item.item_id} className="border-b">
                                <td className="p-2">{idx + 1}</td>
                                <td className="p-2 font-mono text-xs">{item.item_id}</td>
                                <td className="p-2 text-right">{item.count.toLocaleString()}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
              </Card>
                )}
              </div>
            </TabsContent>

            {/* Data Cleaning Tab */}
            <TabsContent value="cleaning" className="space-y-6">
              {/* Reviews Cleaning Section */}
              {cleaningStats?.reviews && (
                <div className="space-y-6">
                  <h2 className="text-2xl font-bold">Reviews Cleaning Process</h2>
                  
                  <div className="grid gap-6 md:grid-cols-2">
                    {/* Retention Rate by Task */}
                    {cleaningStats.reviews.retention_by_task && (
                      <Card>
                        <CardHeader>
                          <CardTitle>Retention Rate Through Cleaning Steps</CardTitle>
                          <CardDescription>Percentage of records retained after each cleaning task</CardDescription>
                        </CardHeader>
                        <CardContent className="p-6">
                          <div className="w-full" style={{ height: '350px' }}>
                            <ChartContainer config={chartConfig} className="w-full h-full">
                              <AreaChart data={cleaningStats.reviews.retention_by_task}>
                                <CartesianGrid strokeDasharray="3 3" />
                                <XAxis dataKey="task" angle={-45} textAnchor="end" height={80} />
                                <YAxis label={{ value: 'Percentage (%)', angle: -90, position: 'insideLeft' }} />
                                <ChartTooltip content={<ChartTooltipContent />} />
                                <Area 
                                  type="monotone" 
                                  dataKey="percentage" 
                                  stroke="#3b82f6" 
                                  fill="#3b82f6" 
                                  fillOpacity={0.3}
                                />
                              </AreaChart>
                            </ChartContainer>
                          </div>
                        </CardContent>
                      </Card>
                    )}

                    {/* Records Dropped by Task */}
                    {cleaningStats.reviews.tasks && (
                      <Card>
                        <CardHeader>
                          <CardTitle>Records Dropped by Cleaning Task</CardTitle>
                          <CardDescription>Number of records removed in each cleaning step</CardDescription>
                        </CardHeader>
                        <CardContent className="p-6">
                          <div className="w-full" style={{ height: '350px' }}>
                            <ChartContainer config={chartConfig} className="w-full h-full">
                              <BarChart data={[
                                { task: "Missing Values", dropped: cleaningStats.reviews.tasks.task1_missing_values.dropped },
                                { task: "Sanity Check", dropped: cleaningStats.reviews.tasks.task2_sanity_check.dropped },
                                { task: "Deduplication", dropped: cleaningStats.reviews.tasks.task3_deduplication.dropped },
                              ]}>
                                <CartesianGrid strokeDasharray="3 3" />
                                <XAxis dataKey="task" angle={-45} textAnchor="end" height={80} />
                                <YAxis />
                                <ChartTooltip content={<ChartTooltipContent />} />
                                <Bar dataKey="dropped" radius={[8, 8, 0, 0]}>
                                  <Cell fill="#ef4444" />
                                  <Cell fill="#f59e0b" />
                                  <Cell fill="#ec4899" />
                                </Bar>
                              </BarChart>
                            </ChartContainer>
                          </div>
                        </CardContent>
                      </Card>
                    )}
                  </div>

                  <div className="grid gap-6 md:grid-cols-2">
                    {/* Rating Distribution Before/After */}
                    {cleaningStats.reviews.rating_distribution_before && cleaningStats.reviews.rating_distribution_after && (
                      <Card>
                        <CardHeader>
                          <CardTitle>Rating Distribution: Before vs After</CardTitle>
                          <CardDescription>Comparison of rating distribution before and after cleaning</CardDescription>
                        </CardHeader>
                        <CardContent className="p-6">
                          <div className="w-full" style={{ height: '350px' }}>
                            <ChartContainer config={chartConfig} className="w-full h-full">
                              <BarChart data={[1, 2, 3, 4, 5].map(rating => {
                                const before = cleaningStats.reviews.rating_distribution_before.find((r: any) => r.rating === rating);
                                const after = cleaningStats.reviews.rating_distribution_after.find((r: any) => r.rating === rating);
                                return {
                                  rating: rating,
                                  before: before?.count || 0,
                                  after: after?.count || 0
                                };
                              })}>
                                <CartesianGrid strokeDasharray="3 3" />
                                <XAxis dataKey="rating" />
                                <YAxis />
                                <ChartTooltip content={<ChartTooltipContent />} />
                                <Legend />
                                <Bar dataKey="before" name="Before Cleaning" fill="#ef4444" radius={[8, 8, 0, 0]} />
                                <Bar dataKey="after" name="After Cleaning" fill="#10b981" radius={[8, 8, 0, 0]} />
                              </BarChart>
                            </ChartContainer>
                          </div>
                        </CardContent>
                      </Card>
                    )}

                    {/* Missing Values Breakdown */}
                    {cleaningStats.reviews.missing_values_breakdown && (
                      <Card>
                        <CardHeader>
                          <CardTitle>Missing Values Breakdown</CardTitle>
                          <CardDescription>Number of missing values by field before cleaning</CardDescription>
                        </CardHeader>
                        <CardContent className="p-6">
                          <div className="w-full" style={{ height: '350px' }}>
                            <ChartContainer config={chartConfig} className="w-full h-full">
                              <BarChart data={Object.entries(cleaningStats.reviews.missing_values_breakdown).map(([key, value]) => ({
                                field: key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
                                count: value
                              }))}>
                                <CartesianGrid strokeDasharray="3 3" />
                                <XAxis dataKey="field" angle={-45} textAnchor="end" height={80} />
                                <YAxis />
                                <ChartTooltip content={<ChartTooltipContent />} />
                                <Bar dataKey="count" fill="#8b5cf6" radius={[8, 8, 0, 0]} />
                              </BarChart>
                            </ChartContainer>
                          </div>
                        </CardContent>
                      </Card>
                    )}
                  </div>

                  {/* Summary Stats */}
                  <Card>
                    <CardHeader>
                      <CardTitle>Reviews Cleaning Summary</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                        <div>
                          <div className="text-sm text-muted-foreground">Initial Records</div>
                          <div className="text-2xl font-bold">{cleaningStats.reviews.before.toLocaleString()}</div>
                        </div>
                        <div>
                          <div className="text-sm text-muted-foreground">Final Records</div>
                          <div className="text-2xl font-bold text-green-600">{cleaningStats.reviews.after.toLocaleString()}</div>
                        </div>
                        <div>
                          <div className="text-sm text-muted-foreground">Total Dropped</div>
                          <div className="text-2xl font-bold text-red-600">{cleaningStats.reviews.dropped.toLocaleString()}</div>
                        </div>
                        <div>
                          <div className="text-sm text-muted-foreground">Retention Rate</div>
                          <div className="text-2xl font-bold">{cleaningStats.reviews.retention_rate.toFixed(2)}%</div>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                </div>
              )}

              {/* 5-Core Interactions Section */}
              {cleaningStats?.interactions_5core && (
                <div className="space-y-6 mt-8">
                  <h2 className="text-2xl font-bold">5-Core Interactions Statistics</h2>
                  
                  <div className="grid gap-6 md:grid-cols-2">
                    {/* Filtering Steps Comparison */}
                    {cleaningStats.interactions_5core.filtering_steps && (
                      <Card>
                        <CardHeader>
                          <CardTitle>Interactions: All vs 5-Core</CardTitle>
                          <CardDescription>Comparison of interactions before and after 5-core filtering</CardDescription>
                        </CardHeader>
                        <CardContent className="p-6">
                          <div className="w-full" style={{ height: '350px' }}>
                            <ChartContainer config={chartConfig} className="w-full h-full">
                              <BarChart data={cleaningStats.interactions_5core.filtering_steps}>
                                <CartesianGrid strokeDasharray="3 3" />
                                <XAxis dataKey="step" />
                                <YAxis />
                                <ChartTooltip content={<ChartTooltipContent />} />
                                <Legend />
                                <Bar dataKey="count" name="Interactions" fill="#3b82f6" radius={[8, 8, 0, 0]} />
                                <Bar dataKey="users" name="Users" fill="#8b5cf6" radius={[8, 8, 0, 0]} />
                                <Bar dataKey="items" name="Items" fill="#ec4899" radius={[8, 8, 0, 0]} />
                              </BarChart>
                            </ChartContainer>
                          </div>
                        </CardContent>
                      </Card>
                    )}

                    {/* Train/Test Split */}
                    {cleaningStats.interactions_5core.train_test_split && (
                      <Card>
                        <CardHeader>
                          <CardTitle>Train/Test Split</CardTitle>
                          <CardDescription>Distribution of interactions in train and test sets</CardDescription>
                        </CardHeader>
                        <CardContent className="p-6">
                          <div className="w-full" style={{ height: '350px' }}>
                            <ChartContainer config={chartConfig} className="w-full h-full">
                              <PieChart>
                                <Pie
                                  data={[
                                    { name: "Train", value: cleaningStats.interactions_5core.train_test_split.train_count },
                                    { name: "Test", value: cleaningStats.interactions_5core.train_test_split.test_count }
                                  ]}
                                  cx="50%"
                                  cy="50%"
                                  labelLine={false}
                                  label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(1)}%`}
                                  outerRadius={100}
                                  fill="#8884d8"
                                  dataKey="value"
                                >
                                  <Cell fill="#3b82f6" />
                                  <Cell fill="#10b981" />
                                </Pie>
                                <ChartTooltip content={<ChartTooltipContent />} />
                              </PieChart>
                            </ChartContainer>
                          </div>
                          <div className="mt-4 space-y-2 text-sm">
                            <div className="flex justify-between">
                              <span>Train:</span>
                              <span className="font-semibold">{cleaningStats.interactions_5core.train_test_split.train_count.toLocaleString()}</span>
                            </div>
                            <div className="flex justify-between">
                              <span>Test:</span>
                              <span className="font-semibold">{cleaningStats.interactions_5core.train_test_split.test_count.toLocaleString()}</span>
                            </div>
                          </div>
                        </CardContent>
                      </Card>
                    )}
                  </div>

                  <div className="grid gap-6 md:grid-cols-2">
                    {/* Rating Distribution in 5-Core */}
                    {cleaningStats.interactions_5core.rating_distribution && (
                      <Card>
                        <CardHeader>
                          <CardTitle>Rating Distribution (5-Core)</CardTitle>
                          <CardDescription>Distribution of ratings in 5-core dataset</CardDescription>
                        </CardHeader>
                        <CardContent className="p-6">
                          <div className="w-full" style={{ height: '350px' }}>
                            <ChartContainer config={chartConfig} className="w-full h-full">
                              <BarChart data={cleaningStats.interactions_5core.rating_distribution}>
                                <CartesianGrid strokeDasharray="3 3" />
                                <XAxis dataKey="rating" />
                                <YAxis />
                                <ChartTooltip content={<ChartTooltipContent />} />
                                <Bar dataKey="count" fill="#06b6d4" radius={[8, 8, 0, 0]} />
                              </BarChart>
                            </ChartContainer>
                          </div>
                        </CardContent>
                      </Card>
                    )}

                    {/* Retention Rates */}
                    {cleaningStats.interactions_5core.core_interactions && (
                      <Card>
                        <CardHeader>
                          <CardTitle>Retention Rates</CardTitle>
                          <CardDescription>Percentage of data retained after 5-core filtering</CardDescription>
                        </CardHeader>
                        <CardContent className="p-6">
                          <div className="w-full" style={{ height: '350px' }}>
                            <ChartContainer config={chartConfig} className="w-full h-full">
                              <BarChart data={[
                                { metric: "Interactions", rate: cleaningStats.interactions_5core.core_interactions.retention_rate },
                                { metric: "Users", rate: cleaningStats.interactions_5core.core_interactions.users_retention_rate },
                                { metric: "Items", rate: cleaningStats.interactions_5core.core_interactions.items_retention_rate }
                              ]}>
                                <CartesianGrid strokeDasharray="3 3" />
                                <XAxis dataKey="metric" />
                                <YAxis label={{ value: 'Retention Rate (%)', angle: -90, position: 'insideLeft' }} />
                                <ChartTooltip content={<ChartTooltipContent />} />
                                <Bar dataKey="rate" fill="#84cc16" radius={[8, 8, 0, 0]} />
                              </BarChart>
                            </ChartContainer>
                          </div>
                        </CardContent>
                      </Card>
                    )}
                  </div>

                  {/* Summary Stats */}
                  <Card>
                    <CardHeader>
                      <CardTitle>5-Core Interactions Summary</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                        <div>
                          <div className="text-sm text-muted-foreground">All Interactions</div>
                          <div className="text-2xl font-bold">{cleaningStats.interactions_5core.all_interactions.count.toLocaleString()}</div>
                        </div>
                        <div>
                          <div className="text-sm text-muted-foreground">5-Core Interactions</div>
                          <div className="text-2xl font-bold text-green-600">{cleaningStats.interactions_5core.core_interactions.count.toLocaleString()}</div>
                        </div>
                        <div>
                          <div className="text-sm text-muted-foreground">Unique Users</div>
                          <div className="text-2xl font-bold">{cleaningStats.interactions_5core.core_interactions.unique_users.toLocaleString()}</div>
                        </div>
                        <div>
                          <div className="text-sm text-muted-foreground">Unique Items</div>
                          <div className="text-2xl font-bold">{cleaningStats.interactions_5core.core_interactions.unique_items.toLocaleString()}</div>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                </div>
              )}

              {/* Embedding Tokens Section */}
              {cleaningStats?.embedding_tokens && (
                <div className="space-y-6 mt-8">
                  <h2 className="text-2xl font-bold">Embedding Token Statistics</h2>
                  
                  <div className="grid gap-6 md:grid-cols-2">
                    {/* Token Distribution */}
                    {cleaningStats.embedding_tokens.token_distribution && (
                      <Card>
                        <CardHeader>
                          <CardTitle>Token Distribution</CardTitle>
                          <CardDescription>Distribution of token counts in embedding text</CardDescription>
                        </CardHeader>
                        <CardContent className="p-6">
                          <div className="w-full" style={{ height: '350px' }}>
                            <ChartContainer config={chartConfig} className="w-full h-full">
                              <BarChart data={cleaningStats.embedding_tokens.token_distribution}>
                                <CartesianGrid strokeDasharray="3 3" />
                                <XAxis dataKey="range" />
                                <YAxis />
                                <ChartTooltip content={<ChartTooltipContent />} />
                                <Bar dataKey="count" fill="#f59e0b" radius={[8, 8, 0, 0]} />
                              </BarChart>
                            </ChartContainer>
                          </div>
                        </CardContent>
                      </Card>
                    )}

                    {/* Token Statistics */}
                    {cleaningStats.embedding_tokens.statistics && (
                      <Card>
                        <CardHeader>
                          <CardTitle>Token Statistics</CardTitle>
                          <CardDescription>Statistical summary of token counts</CardDescription>
                        </CardHeader>
                        <CardContent className="p-6">
                          <div className="space-y-4">
                            <div className="grid grid-cols-2 gap-4">
                              <div>
                                <div className="text-sm text-muted-foreground">Average Tokens</div>
                                <div className="text-2xl font-bold">{cleaningStats.embedding_tokens.statistics.avg_tokens.toFixed(1)}</div>
                              </div>
                              <div>
                                <div className="text-sm text-muted-foreground">Median Tokens</div>
                                <div className="text-2xl font-bold">{cleaningStats.embedding_tokens.statistics.median_tokens.toFixed(1)}</div>
                              </div>
                              <div>
                                <div className="text-sm text-muted-foreground">Min Tokens</div>
                                <div className="text-2xl font-bold">{cleaningStats.embedding_tokens.statistics.min_tokens}</div>
                              </div>
                              <div>
                                <div className="text-sm text-muted-foreground">Max Tokens</div>
                                <div className="text-2xl font-bold">{cleaningStats.embedding_tokens.statistics.max_tokens}</div>
                              </div>
                              <div className="col-span-2">
                                <div className="text-sm text-muted-foreground">Std Deviation</div>
                                <div className="text-2xl font-bold">{cleaningStats.embedding_tokens.statistics.std_tokens.toFixed(1)}</div>
                              </div>
                            </div>
                          </div>
                        </CardContent>
                      </Card>
                    )}
                  </div>

                  {/* Summary Stats */}
                  <Card>
                    <CardHeader>
                      <CardTitle>Embedding Tokens Summary</CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
                        <div>
                          <div className="text-sm text-muted-foreground">Total Items</div>
                          <div className="text-2xl font-bold">{cleaningStats.embedding_tokens.total_items.toLocaleString()}</div>
                        </div>
                        <div>
                          <div className="text-sm text-muted-foreground">Avg Tokens per Item</div>
                          <div className="text-2xl font-bold">{cleaningStats.embedding_tokens.statistics.avg_tokens.toFixed(0)}</div>
                        </div>
                        <div>
                          <div className="text-sm text-muted-foreground">Total Tokens (approx)</div>
                          <div className="text-2xl font-bold">{(cleaningStats.embedding_tokens.total_items * cleaningStats.embedding_tokens.statistics.avg_tokens).toLocaleString()}</div>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                </div>
              )}

              {/* Embedding Stats */}
              {embeddingStats && (
                <div className="mt-8">
                  <h2 className="text-2xl font-bold mb-6">Embedding Statistics</h2>
                  <Card>
                    <CardHeader>
                      <CardTitle>Embedding Statistics</CardTitle>
                      <CardDescription>Thống kê về dữ liệu embedding</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <div className="text-sm text-muted-foreground">Total Items</div>
                          <div className="text-2xl font-bold">{embeddingStats.total_items?.toLocaleString()}</div>
                        </div>
                        <div>
                          <div className="text-sm text-muted-foreground">Categories</div>
                          <div className="text-2xl font-bold">{embeddingStats.unique_categories || 'N/A'}</div>
                        </div>
                      </div>
                      <div>
                        <div className="text-sm text-muted-foreground mb-2">Columns:</div>
                        <div className="flex flex-wrap gap-2">
                          {embeddingStats.columns?.map((col: string) => (
                            <span key={col} className="px-2 py-1 bg-muted rounded text-xs font-mono">
                              {col}
                            </span>
                          ))}
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                </div>
              )}
            </TabsContent>
          </Tabs>
        </div>
      </main>
      <Footer />
    </div>
  )
}
