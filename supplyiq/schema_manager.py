"""
Database Schema Manager for Supply Chain Analytics
Provides schema information and metadata for text-to-SQL generation
"""

class SchemaManager:
    def __init__(self):
        self.schema_info = {
            # 1st sheet: Strategic Data
            "strategic_data": {
                "table_name": "Strategic Data",
                "description": "Main strategic business data with forecasts and actuals",
                "columns": {
                    "Time Period_Text": {"type": "TEXT", "description": "Time period in 'Jan-22' format for easy matching"},
                    "Time Period": {"type": "TEXT", "description": "Time period identifier (e.g., Jan-22)"},
                    "Year": {"type": "INTEGER", "description": "Year (2022, 2023, etc.)"},
                    "Month": {"type": "INTEGER", "description": "Month number (1-12)"},
                    "Product Category": {"type": "TEXT", "description": "Product categories: GROOMING, SNACKS, WASHCARE, DESSERT, NAMKEEN"},
                    "Year Target": {"type": "REAL", "description": "Annual target percentage"},
                    "Month Split": {"type": "REAL", "description": "Monthly split percentage"},
                    "Monthly Revenue": {"type": "REAL", "description": "Monthly revenue percentage"},
                    "Cat Split": {"type": "REAL", "description": "Category split percentage"},
                    "Business Forecast": {"type": "INTEGER", "description": "Business forecast value"},
                    "Actual": {"type": "INTEGER", "description": "Actual sales value"},
                    "Unit Of Measurement": {"type": "TEXT", "description": "Currency code (e.g., USD)"}
                }
            },
            # 2nd sheet: order date
            "order_date": {
                "table_name": "Order Date",
                "description": "Customer order data with delivery information",
                "columns": {
                    "ORDER_DATE_Text": {"type": "TEXT", "description": "Order date in 'Jan-22' format for easy matching"},
                    "Year": {"type": "INTEGER", "description": "Order year"},
                    "FACILITY_ID": {"type": "INTEGER", "description": "Facility identifier"},
                    "REGION": {"type": "TEXT", "description": "Region code (NAM, etc.)"},
                    "MATERIAL_ID": {"type": "TEXT", "description": "Material/Product ID"},
                    "Product Category": {"type": "TEXT", "description": "Product category"},
                    "CUSTMER_NAME": {"type": "TEXT", "description": "Customer name"},
                    "Customer Requested QTY": {"type": "INTEGER", "description": "Quantity requested by customer"},
                    "Delivered QTY": {"type": "INTEGER", "description": "Quantity delivered"},
                    "Customer service level": {"type": "REAL", "description": "Service level percentage"}
                }
            },
            # 3rd sheet: customer shipment
            "customer_shipment": {
                "table_name": "Customer Shipment",
                "description": "Customer shipment tracking data",
                "columns": {
                    "SHIPMENT_TIME_PERIOD_ID_Text": {"type": "TEXT", "description": "Shipment time period in 'Jan-22' format for easy matching"},
                    "SHIPMENT_TIME_PERIOD_ID": {"type": "TEXT", "description": "Shipment time period"},
                    "Month": {"type": "INTEGER", "description": "Shipment month"},
                    "Year": {"type": "INTEGER", "description": "Shipment year"},
                    "REGION": {"type": "TEXT", "description": "Region code"},
                    "FACILITY_ID": {"type": "INTEGER", "description": "Facility ID"},
                    "MATERIAL_ID": {"type": "TEXT", "description": "Material ID"},
                    "Product Category": {"type": "TEXT", "description": "Product category"},
                    "CUST_NAMME": {"type": "TEXT", "description": "Customer name"},
                    "agg": {"type": "TEXT", "description": "Aggregate key"},
                    "SHIPMENT_QTY": {"type": "INTEGER", "description": "Shipped quantity"},
                    "QTY_UOM_CD": {"type": "TEXT", "description": "Unit of measure (CS, etc.)"}
                }
            },
            # 4th sheet: material forecast
            "material_forecast": {
                "table_name": "Material Forecast",
                "description": "Material forecasting data with multiple forecast types",
                "columns": {
                    "TIME_PERIOD_ID_Text": {"type": "TEXT", "description": "Forecast time period in 'Jan-22' format for easy matching"},
                    "FORECAST_PERIOD_Text": {"type": "TEXT", "description": "Forecast period in 'Jan-22' format for easy matching"},
                    "TIME_PERIOD_ID": {"type": "TEXT", "description": "Time period identifier"},
                    "FORECAST_PERIOD": {"type": "TEXT", "description": "Forecast period"},
                    "Month": {"type": "INTEGER", "description": "Month"},
                    "Year": {"type": "INTEGER", "description": "Year"},
                    "REGION": {"type": "TEXT", "description": "Region"},
                    "FACILITY_ID": {"type": "INTEGER", "description": "Facility ID"},
                    "MATERIAL_ID": {"type": "TEXT", "description": "Material ID"},
                    "Product Category": {"type": "TEXT", "description": "Product category"},
                    "CUSTMER_NAME": {"type": "TEXT", "description": "Customer name"},
                    "agg": {"type": "TEXT", "description": "Aggregate key"},
                    "BASELINE_FORECAST": {"type": "INTEGER", "description": "Baseline forecast quantity"},
                    "SALES_FORECAST": {"type": "INTEGER", "description": "Sales forecast quantity"},
                    "PROMOTION_ADJUSTMENT": {"type": "INTEGER", "description": "Promotion adjustment"},
                    "PROMOTION_FORECAST": {"type": "INTEGER", "description": "Promotion forecast"},
                    "CONSENSUS_FORECAST": {"type": "INTEGER", "description": "Consensus forecast"},
                    "SHIPMENT QTY": {"type": "INTEGER", "description": "Actual shipment quantity"},
                    "QTY_UOM_CD": {"type": "TEXT", "description": "Unit of measure (CS, etc.)"},
                    "UNIT_PRICE_AMT": {"type": "INTEGER", "description": "Unit price amount"},
                    "CURRENCY_CD": {"type": "TEXT", "description": "Currency code"},
                    "FORECAST_PERIOD_INDICATOR": {"type": "TEXT", "description": "Forecast period indicator"},
                    "ACTIVE_FLG": {"type": "TEXT", "description": "Active flag"},
                    "PRODUCT_CATEGORY": {"type": "TEXT", "description": "Product category duplicate"}
                }
            },
            # 5th sheet: promotion
            "promotion": {
                "table_name": "Promotion",
                "description": "Promotional campaigns and offers data",
                "columns": {
                    "PROMOTION_START_DT_Text": {"type": "TEXT", "description": "Promotion start date in 'Jan-22' format for easy matching. To filter for 2023, use LIKE '%23' on this column."},
                    "PROMOTION_END_DT_Text": {"type": "TEXT", "description": "Promotion end date in 'Jan-22' format for easy matching. To filter for 2023, use LIKE '%23' on this column."},
                    "TRADE_PROMOTION_TYPE": {"type": "TEXT", "description": "Type of promotion"},
                    "PROMOTION_START_DT": {"type": "TEXT", "description": "Promotion start date"},
                    "PROMOTION_END_DT": {"type": "TEXT", "description": "Promotion end date"},
                    "FACILITY": {"type": "INTEGER", "description": "Facility ID"},
                    "MATERIAL": {"type": "TEXT", "description": "Material ID"},
                    "CUSTOMER": {"type": "TEXT", "description": "Customer name"},
                    "SALES_UPLIFT%": {"type": "TEXT", "description": "Sales uplift percentage (e.g., '61%')"}
                }
            },
            # 6th sheet: facility material inventory
            "facility_material_inventory": {
                "table_name": "Facility Material Inventory",
                "description": "Inventory levels and values by facility and material",
                "columns": {
                    "TIME_PERIOD_ID_Text": {"type": "TEXT", "description": "Time period in 'Jan-22' format for easy matching"},
                    "Key": {"type": "TEXT", "description": "Unique key identifier"},
                    "FACILITY_ID": {"type": "INTEGER", "description": "Facility ID"},
                    "REGION": {"type": "TEXT", "description": "Region code"},
                    "MATERIAL_ID": {"type": "TEXT", "description": "Material ID"},
                    "Product Category": {"type": "TEXT", "description": "Product category"},
                    "TIME_PERIOD_ID": {"type": "TEXT", "description": "Time period identifier"},
                    "Month": {"type": "INTEGER", "description": "Month"},
                    "Year": {"type": "INTEGER", "description": "Year"},
                    "CLOSING_INVENTORY_QTY": {"type": "INTEGER", "description": "Closing inventory quantity"},
                    "Unit Price": {"type": "INTEGER", "description": "Unit price"},
                    "Inventory value": {"type": "INTEGER", "description": "Total inventory value"},
                    "SAFETY_STOCK": {"type": "INTEGER", "description": "Safety stock level"},
                    "QTY_UOM_CD": {"type": "TEXT", "description": "Quantity unit of measure"}
                }
            },
            # 7th sheet: lost sale
            "lost_sale": {
                "table_name": "Lost Sale",
                "description": "Lost sales data with reasons and impact",
                "columns": {
                    "Lost_Month_Text": {"type": "TEXT", "description": "Lost month in 'Jan-22' format for easy matching"},
                    "Lost_Month": {"type": "TEXT", "description": "Month when sale was lost"},
                    "Facility": {"type": "INTEGER", "description": "Facility ID"},
                    "Material": {"type": "TEXT", "description": "Material ID"},
                    "Customer": {"type": "TEXT", "description": "Customer name"},
                    "Loss_Tree_Level1": {"type": "TEXT", "description": "Primary loss reason category"},
                    "Loss_Tree_Level2": {"type": "TEXT", "description": "Secondary loss reason"},
                    "ABCD": {"type": "TEXT", "description": "Priority classification (A, B, C, D)"},
                    "Unit_Lost": {"type": "INTEGER", "description": "Units lost"},
                    "Unit_Price": {"type": "INTEGER", "description": "Unit price"},
                    "losess_value": {"type": "TEXT", "description": "Total loss value"},
                    "UOM": {"type": "TEXT", "description": "Unit of measure"}
                }
            }
        }

        # Common product categories across tables
        self.product_categories = ["GROOMING", "SNACKS", "WASHCARE", "DESSERT", "NAMKEEN"]

        # Common customers
        self.customers = [
            "GIANGARLO SCIENTIFIC CO", "J F HUBERT BATTER", "BROADSPIRE SERVICES",
            "ROCKWELL PINE", "QMC LLC", "COLOR CRAFT", "ATACS PRODUCTS",
            "CLARK FOAM PRODUCTS CORPORATION", "ROFIN SINAR", "RJR", "IFM EFECTOR"
        ]

    def get_schema_context(self):
        """Get complete schema context for LLM"""
        # BUG FIX: Original code used += without '\n', mashing all text together.
        # Now using a list and joining with newlines for proper readable schema output.
        lines = ["=== DATABASE SCHEMA ==="]

        for table_key, table_info in self.schema_info.items():
            lines.append(f"\nTABLE: {table_info['table_name']}")
            lines.append(f"Description: {table_info['description']}")
            lines.append("Columns:")
            for col_name, col_info in table_info['columns'].items():
                lines.append(f"  - {col_name} ({col_info['type']}): {col_info['description']}")

        lines.append("\n=== COMMON VALUES ===")
        lines.append(f"Product Categories: {', '.join(self.product_categories)}")
        lines.append(f"Sample Customers: {', '.join(self.customers[:5])}...")
        lines.append("Years Available: 2022, 2023")

        return "\n".join(lines)

    def get_table_info(self, table_name):
        """Get specific table information"""
        for table_key, table_info in self.schema_info.items():
            if table_info['table_name'].lower() == table_name.lower():
                return table_info
        return None

    def get_all_table_names(self):
        """Get all available table names"""
        return [info['table_name'] for info in self.schema_info.values()]

    def validate_table_name(self, table_key):
        """Check if table name exists"""
        return table_key.lower() in [info['table_name'].lower() for info in self.schema_info.values()]

    def get_columns_for_table(self, table_name):
        """Get column names for a specific table"""
        table_info = self.get_table_info(table_name)
        return list(table_info['columns'].keys()) if table_info else []
