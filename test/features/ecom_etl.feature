Feature: E-commerce Enrichment Pipeline

  Scenario: Enrich orders with customer and product data
    Given the following cleaned orders:
      | order_id | customer_id | product_id | profit | order_date |
      | O1       | C1          | P1         | 12.345 | 2024-01-01 |
    And the following customers:
      | customer_id | customer_name | country  |
      | C1          | Alice         | US       |
    And the following products:
      | product_id | category | sub_category |
      | P1         | Tech     | Phones       |
    When the enriched dataset is created
    Then the result should contain:
      | order_id | customer_name | country | category | sub_category | profit |
      | O1       | Alice         | US      | Tech     | Phones       | 12.35  |
