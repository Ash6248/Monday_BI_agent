"""
Query Processor Module for Monday.com BI Agent
Parses natural language queries and generates business intelligence insights.
"""
import re
import logging
from data_loader import (
    get_deals_data, 
    get_work_orders_data, 
    query_deals_by_sector,
    query_deals_by_stage, 
    get_pipeline_summary, 
    get_sector_performance,
    get_data_quality_notes
)

# Configure logging
logger = logging.getLogger(__name__)


class QueryProcessor:
    """Processes natural language queries and generates business insights."""
    
    def __init__(self):
        self.action_log = []
        logger.info("QueryProcessor initialized")
    
    def log_action(self, action):
        """Log an action for visibility."""
        self.action_log.append(action)
        logger.debug(f"[ACTION] {action}")
    
    def parse_query(self, query):
        """Parse a natural language query to extract intent and entities."""
        query_lower = query.lower().strip()
        self.log_action(f"Parsing query: '{query_lower}'")
        
        # Define sector keywords
        sectors = ['mining', 'powerline', 'renewables', 'dsp', 'railways', 'tender', 'energy']
        
        # Define stage keywords
        stages = ['sales qualified', 'feasibility', 'proposal', 'negotiations', 
                  'work order', 'on hold', 'closed']
        
        # Define time keywords
        quarters = ['q1', 'q2', 'q3', 'q4', 'quarter']
        years = ['2024', '2025', '2026']
        
        # Extract entities
        found_sectors = [s for s in sectors if s in query_lower]
        found_stages = [s for s in stages if s in query_lower]
        found_quarters = [q for q in quarters if q in query_lower]
        found_years = [y for y in years if y in query_lower]
        
        # Determine intent
        intent = 'general'
        if any(w in query_lower for w in ['pipeline', 'deals', 'opportunities']):
            intent = 'pipeline'
        elif any(w in query_lower for w in ['revenue', 'value', 'worth', 'money']):
            intent = 'revenue'
        elif any(w in query_lower for w in ['sector', 'industry', 'vertical']):
            intent = 'sector'
        elif any(w in query_lower for w in ['performance', 'how', 'looking']):
            intent = 'performance'
        
        result = {
            'intent': intent,
            'sectors': found_sectors,
            'stages': found_stages,
            'quarters': found_quarters,
            'years': found_years,
            'original_query': query_lower
        }
        
        self.log_action(f"Parsed intent: {intent}, sectors: {found_sectors}, stages: {found_stages}")
        logger.info(f"Query parsed: intent={intent}, sectors={found_sectors}")
        return result
    
    def process_query(self, query):
        """Process a query and return insights."""
        self.action_log = []  # Reset log
        logger.info(f"Processing query: {query}")
        
        parsed = self.parse_query(query)
        
        deals_df = get_deals_data()
        if deals_df is None or len(deals_df) == 0:
            logger.warning("No deal data available")
            return {
                'error': 'No deal data available. Please ensure data files are loaded.',
                'actions': self.action_log
            }
        
        try:
            # Route based on intent
            intent = parsed['intent']
            if intent == 'pipeline':
                return self.handle_pipeline_query(parsed, deals_df)
            elif intent == 'revenue':
                return self.handle_revenue_query(parsed, deals_df)
            elif intent == 'sector':
                return self.handle_sector_query(parsed, deals_df)
            elif intent == 'performance':
                return self.handle_performance_query(parsed, deals_df)
            else:
                return self.handle_general_query(parsed, deals_df)
        except Exception as e:
            logger.error(f"Error processing query: {e}", exc_info=True)
            return {
                'error': 'Error processing query: ' + str(e),
                'actions': self.action_log
            }
    
    def handle_pipeline_query(self, parsed, df):
        """Handle pipeline-related queries."""
        self.log_action("Executing pipeline query handler")
        
        # Check if filtering by sector
        sectors = parsed.get('sectors', [])
        if sectors:
            sector = sectors[0]
            # Map energy to renewables for this dataset
            if sector == 'energy':
                sector = 'renewables'
            filtered = query_deals_by_sector(df, sector)
            summary = get_pipeline_summary(filtered)
            response = "**Pipeline Overview for " + sector.title() + " Sector:**\n\n"
            response += "- Total Deals: " + str(summary['total_deals']) + "\n"
            response += "- Total Value: $" + f"{summary['total_value']:,.2f}" + "\n"
            response += "- Open Deals: " + str(summary['open_deals']) + "\n"
            response += "- On Hold: " + str(summary['on_hold']) + "\n"
            response += "- Closed: " + str(summary['closed'])
        else:
            summary = get_pipeline_summary(df)
            response = "**Overall Pipeline Overview:**\n\n"
            response += "- Total Deals: " + str(summary['total_deals']) + "\n"
            response += "- Total Value: $" + f"{summary['total_value']:,.2f}" + "\n"
            response += "- Open Deals: " + str(summary['open_deals']) + "\n"
            response += "- On Hold: " + str(summary['on_hold']) + "\n"
            response += "- Closed: " + str(summary['closed'])
        
        return {'response': response, 'actions': self.action_log}
    
    def handle_revenue_query(self, parsed, df):
        """Handle revenue-related queries."""
        self.log_action("Executing revenue query handler")
        
        sectors = parsed.get('sectors', [])
        if sectors:
            sector = sectors[0]
            if sector == 'energy':
                sector = 'renewables'
            filtered = query_deals_by_sector(df, sector)
            total_value = filtered['Masked Deal value'].sum()
            response = "**Revenue Analysis for " + sector.title() + " Sector:**\n\n"
            response += "- Total Pipeline Value: $" + f"{total_value:,.2f}" + "\n"
            response += "- Number of Deals: " + str(len(filtered)) + "\n"
            if len(filtered) > 0:
                response += "- Average Deal Value: $" + f"{total_value/len(filtered):,.2f}"
        else:
            total_value = df['Masked Deal value'].sum()
            response = "**Overall Revenue Analysis:**\n\n"
            response += "- Total Pipeline Value: $" + f"{total_value:,.2f}" + "\n"
            response += "- Number of Deals: " + str(len(df)) + "\n"
            response += "- Average Deal Value: $" + f"{total_value/len(df):,.2f}"
        
        return {'response': response, 'actions': self.action_log}
    
    def handle_sector_query(self, parsed, df):
        """Handle sector-related queries."""
        self.log_action("Executing sector query handler")
        
        sectors = parsed.get('sectors', [])
        if sectors:
            sector = sectors[0]
            if sector == 'energy':
                sector = 'renewables'
            filtered = query_deals_by_sector(df, sector)
            summary = get_pipeline_summary(filtered)
            response = "**" + sector.title() + " Sector Performance:**\n\n"
            response += "- Total Deals: " + str(summary['total_deals']) + "\n"
            response += "- Total Value: $" + f"{summary['total_value']:,.2f}" + "\n"
            response += "- Open: " + str(summary['open_deals']) + ", On Hold: " + str(summary['on_hold']) + ", Closed: " + str(summary['closed'])
        else:
            sector_perf = get_sector_performance(df)
            response = "**Sector Performance Summary:**\n\n"
            for _, row in sector_perf.iterrows():
                response += "**" + str(row['Sector/service']) + ":**\n"
                response += "  - Total Value: $" + f"{row['Total Value']:,.2f}" + "\n"
                response += "  - Deal Count: " + str(int(row['Deal Count'])) + "\n"
                response += "  - Open Deals: " + str(int(row['Open Deals'])) + "\n\n"
        
        return {'response': response, 'actions': self.action_log}
    
    def handle_performance_query(self, parsed, df):
        """Handle performance-related queries."""
        self.log_action("Executing performance query handler")
        
        # If sector specified, show sector performance
        sectors = parsed.get('sectors', [])
        if sectors:
            sector = sectors[0]
            if sector == 'energy':
                sector = 'renewables'
            filtered = query_deals_by_sector(df, sector)
            stage_dist = filtered['Deal Stage'].value_counts()
            response = "**Performance - " + sector.title() + " Sector:**\n\n"
            response += "Deal Stage Distribution:\n"
            for stage, count in stage_dist.items():
                response += "- " + str(stage) + ": " + str(count) + " deals\n"
        else:
            sector_perf = get_sector_performance(df)
            response = "**Overall Business Performance:**\n\n"
            response += "Total Pipeline: $" + f"{df['Masked Deal value'].sum():,.2f}" + "\n"
            response += "Total Deals: " + str(len(df)) + "\n\n"
            response += "**Top Sectors by Value:**\n"
            top_sectors = sector_perf.nlargest(3, 'Total Value')
            for _, row in top_sectors.iterrows():
                response += "- " + str(row['Sector/service']) + ": $" + f"{row['Total Value']:,.2f}" + "\n"
        
        return {'response': response, 'actions': self.action_log}
    
    def handle_general_query(self, parsed, df):
        """Handle general queries."""
        self.log_action("Executing general query handler")
        
        summary = get_pipeline_summary(df)
        sector_perf = get_sector_performance(df)
        
        response = "**Business Intelligence Summary:**\n\n"
        response += "Here's what I found:\n\n"
        response += "**Pipeline:** " + str(summary['total_deals']) + " total deals worth $" + f"{summary['total_value']:,.2f}" + "\n"
        response += "**Open Deals:** " + str(summary['open_deals']) + "\n"
        response += "**On Hold:** " + str(summary['on_hold']) + "\n\n"
        
        sectors = parsed.get('sectors', [])
        if not sectors:
            response += "**Available Sectors:**\n"
            for sector in sector_perf['Sector/service'].unique():
                response += "- " + str(sector) + "\n"
            response += "\n*Try asking about a specific sector like 'Mining' or 'Energy'*"
        
        return {'response': response, 'actions': self.action_log}
    
    def get_help(self):
        """Return help information."""
        help_text = """**Monday.com BI Agent - Available Queries:**

Here are some examples of questions you can ask:

**Pipeline Questions:**
- "How's our pipeline looking?"
- "Show me all open deals"
- "What's our pipeline for the energy sector?"

**Revenue Questions:**
- "What's our total revenue?"
- "How much is the mining sector worth?"
- "What's the average deal value?"

**Sector Questions:**
- "How is the renewables sector performing?"
- "Show sector breakdown"
- "Which sector has the most deals?"

**Performance Questions:**
- "How's business looking this quarter?"
- "Show performance by sector"
- "What's the status of all deals?"

**Tips:**
- Specify sectors: Mining, Powerline, Renewables, DSP, Railways, Tender
- Ask about specific stages or statuses
- Request specific time periods (2024, 2025, Q1, Q2, etc.)
"""
        return {'response': help_text, 'actions': self.action_log}
