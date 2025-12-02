from typing import Dict
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
import missingno as msno
from pathlib import Path
from scipy import stats
from data_orm.config.logger import get_logger

logger = get_logger()

class Statistizer:
    """Handles statistical analysis and data visualization"""
    
    def __init__(self):
        self.stats_report = {}
        self.visualization_path = "visualizations"
        Path(self.visualization_path).mkdir(exist_ok=True)
    
    def generate_descriptive_stats(self, df: pd.DataFrame) -> Dict:
        """Generate comprehensive descriptive statistics"""
        logger.info("Generating descriptive statistics...")
        stats = {}
        
        # Basic info
        stats['shape'] = df.shape
        stats['columns'] = list(df.columns)
        stats['dtypes'] = df.dtypes.astype(str).to_dict()
        
        logger.debug(f"Dataset shape: {df.shape}")
        logger.debug(f"Data types: {df.dtypes.to_dict()}")
        
        # Descriptive statistics for numerical columns
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        if len(numerical_cols) > 0:
            stats['numerical_stats'] = df[numerical_cols].describe().to_dict()
            stats['variance'] = df[numerical_cols].var().to_dict()
            stats['skewness'] = df[numerical_cols].skew().to_dict()
            stats['kurtosis'] = df[numerical_cols].kurtosis().to_dict()
            
            logger.info(f"Numerical columns analyzed: {list(numerical_cols)}")
            for col in numerical_cols:
                logger.debug(f"  {col}: mean={df[col].mean():.2f}, std={df[col].std():.2f}")
        
        # Descriptive statistics for categorical columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        if len(categorical_cols) > 0:
            stats['categorical_stats'] = {}
            for col in categorical_cols:
                stats['categorical_stats'][col] = {
                    'unique_count': df[col].nunique(),
                    'most_frequent': df[col].mode().iloc[0] if not df[col].mode().empty else None,
                    'frequency': df[col].value_counts().to_dict()
                }
            logger.info(f"Categorical columns analyzed: {list(categorical_cols)}")
        
        self.stats_report['descriptive_stats'] = stats
        logger.success("Descriptive statistics generated successfully")
        
        return stats
    
    def analyze_missing_values(self, df: pd.DataFrame) -> Dict:
        """Analyze missing values pattern"""
        logger.info("Analyzing missing values...")
        missing_analysis = {}
        
        total_cells = df.size
        missing_cells = df.isnull().sum().sum()
        missing_analysis['overall_missing_percentage'] = (missing_cells / total_cells) * 100
        
        # Missing values by column
        missing_by_column = df.isnull().sum()
        missing_analysis['missing_by_column'] = missing_by_column[missing_by_column > 0].to_dict()
        missing_analysis['missing_percentage_by_column'] = (df.isnull().sum() / len(df) * 100).to_dict()
        
        # Missing values pattern
        missing_analysis['missing_pattern'] = {
            'complete_columns': missing_by_column[missing_by_column == 0].index.tolist(),
            'incomplete_columns': missing_by_column[missing_by_column > 0].index.tolist()
        }
        
        # Log missing values information
        if missing_cells > 0:
            logger.warning(f"Missing values detected: {missing_cells} cells ({missing_analysis['overall_missing_percentage']:.2f}%)")
            for col, count in missing_analysis['missing_by_column'].items():
                percentage = missing_analysis['missing_percentage_by_column'][col]
                logger.warning(f"  {col}: {count} missing values ({percentage:.2f}%)")
        else:
            logger.success("No missing values detected")
        
        self.stats_report['missing_analysis'] = missing_analysis
        return missing_analysis
    
    def create_visualizations(self, df: pd.DataFrame):
        """Create comprehensive visualizations"""
        logger.info("Creating visualizations...")
        try:
            self._create_missing_values_plot(df)
            self._create_distribution_plots(df)
            self._create_scatter_plots(df)  # <-- NOUVEAU : Diagrammes de dispersion
            self._create_correlation_heatmap(df)
            self._create_boxplots(df)
            self._create_pair_plot(df)  # <-- NOUVEAU : Pair plot
            logger.success("All visualizations created successfully")
        except Exception as e:
            logger.error(f"Error creating visualizations: {str(e)}")
    
    def _create_missing_values_plot(self, df: pd.DataFrame):
        """Create missing values visualization"""
        try:
            plt.figure(figsize=(12, 8))
            msno.matrix(df)
            plt.title('Missing Values Matrix')
            plt.savefig(f'{self.visualization_path}/missing_values_matrix.png', 
                       bbox_inches='tight', dpi=300)
            plt.close()
            logger.debug("Missing values matrix plot created")
            
            # Bar plot of missing values
            missing_percent = (df.isnull().sum() / len(df)) * 100
            missing_percent = missing_percent[missing_percent > 0]
            
            if len(missing_percent) > 0:
                plt.figure(figsize=(10, 6))
                missing_percent.sort_values(ascending=False).plot(kind='bar')
                plt.title('Percentage of Missing Values by Column')
                plt.ylabel('Percentage (%)')
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.savefig(f'{self.visualization_path}/missing_values_bar.png', 
                           bbox_inches='tight', dpi=300)
                plt.close()
                logger.debug("Missing values bar plot created")
                
        except Exception as e:
            logger.warning(f"Could not create missing values plot: {str(e)}")
    
    def _create_distribution_plots(self, df: pd.DataFrame):
        """Create distribution plots for numerical columns"""
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        
        for col in numerical_cols:
            try:
                fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(18, 5))
                
                # Histogram with KDE
                df[col].dropna().hist(bins=30, alpha=0.7, edgecolor='black', ax=ax1)
                df[col].dropna().plot.density(ax=ax1, secondary_y=True, color='red')
                ax1.set_title(f'Distribution of {col}')
                ax1.set_xlabel(col)
                ax1.set_ylabel('Frequency')
                
                # Q-Q plot
                stats.probplot(df[col].dropna(), dist="norm", plot=ax2)
                ax2.set_title(f'Q-Q Plot of {col}')
                
                # Box plot
                df.boxplot(column=col, ax=ax3)
                ax3.set_title(f'Box Plot of {col}')
                
                plt.tight_layout()
                plt.savefig(f'{self.visualization_path}/distribution_{col}.png', 
                           bbox_inches='tight', dpi=300)
                plt.close()
                logger.debug(f"Distribution plot created for {col}")
                
            except Exception as e:
                logger.warning(f"Could not create distribution plot for {col}: {str(e)}")
    
    def _create_scatter_plots(self, df: pd.DataFrame):
        """Create scatter plots for numerical variable pairs"""
        numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        if len(numerical_cols) < 2:
            logger.info("Not enough numerical columns for scatter plots")
            return
        
        # Create scatter plots for top correlated pairs
        correlation_matrix = df[numerical_cols].corr()
        
        # Find highly correlated pairs (absolute correlation > 0.5)
        highly_correlated_pairs = []
        for i in range(len(numerical_cols)):
            for j in range(i + 1, len(numerical_cols)):
                corr = abs(correlation_matrix.iloc[i, j])
                if corr > 0.3:  # Lower threshold to get more pairs
                    highly_correlated_pairs.append((
                        numerical_cols[i], 
                        numerical_cols[j], 
                        correlation_matrix.iloc[i, j]
                    ))
        
        # Sort by absolute correlation
        highly_correlated_pairs.sort(key=lambda x: abs(x[2]), reverse=True)
        
        # Create scatter plots for top pairs (max 12 plots)
        top_pairs = highly_correlated_pairs[:12]
        
        if not top_pairs:
            logger.info("No highly correlated pairs found for scatter plots")
            return
        
        # Create 3x4 grid of scatter plots
        n_plots = len(top_pairs)
        n_cols = 3
        n_rows = (n_plots + n_cols - 1) // n_cols
        
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 5 * n_rows))
        if n_rows == 1:
            axes = [axes] if n_cols == 1 else axes
        else:
            axes = axes.flatten()
        
        for idx, (col1, col2, corr) in enumerate(top_pairs):
            if idx < len(axes):
                ax = axes[idx]
                ax.scatter(df[col1], df[col2], alpha=0.6, s=20)
                ax.set_xlabel(col1)
                ax.set_ylabel(col2)
                ax.set_title(f'{col1} vs {col2}\n(corr: {corr:.3f})')
                
                # Add trend line
                try:
                    z = np.polyfit(df[col1].dropna(), df[col2].dropna(), 1)
                    p = np.poly1d(z)
                    ax.plot(df[col1], p(df[col1]), "r--", alpha=0.8)
                except:
                    pass  # Skip trend line if cannot compute
        
        # Remove empty subplots
        for idx in range(len(top_pairs), len(axes)):
            fig.delaxes(axes[idx])
        
        plt.tight_layout()
        plt.savefig(f'{self.visualization_path}/scatter_plots_correlated.png', 
                   bbox_inches='tight', dpi=300)
        plt.close()
        logger.debug("Scatter plots for correlated pairs created")
        
        # Create interactive scatter plot with plotly
        self._create_interactive_scatter_plot(df, numerical_cols)
    
    def _create_interactive_scatter_plot(self, df: pd.DataFrame, numerical_cols: list):
        """Create interactive scatter plot with plotly"""
        if len(numerical_cols) >= 2:
            try:
                # Use first two numerical columns for basic scatter plot
                fig = px.scatter(
                    df, 
                    x=numerical_cols[0], 
                    y=numerical_cols[1],
                    title=f'Interactive Scatter: {numerical_cols[0]} vs {numerical_cols[1]}',
                    opacity=0.7
                )
                
                # Add color if there's a third numerical column or categorical column
                categorical_cols = df.select_dtypes(include=['object']).columns
                if len(numerical_cols) >= 3:
                    fig = px.scatter(
                        df, 
                        x=numerical_cols[0], 
                        y=numerical_cols[1],
                        color=numerical_cols[2],
                        title=f'Interactive Scatter: {numerical_cols[0]} vs {numerical_cols[1]}',
                        opacity=0.7
                    )
                elif len(categorical_cols) > 0:
                    fig = px.scatter(
                        df, 
                        x=numerical_cols[0], 
                        y=numerical_cols[1],
                        color=categorical_cols[0],
                        title=f'Interactive Scatter: {numerical_cols[0]} vs {numerical_cols[1]}',
                        opacity=0.7
                    )
                
                fig.write_html(f'{self.visualization_path}/interactive_scatter_plot.html')
                logger.debug("Interactive scatter plot created")
                
            except Exception as e:
                logger.warning(f"Could not create interactive scatter plot: {str(e)}")
    
    def _create_pair_plot(self, df: pd.DataFrame):
        """Create pair plot for numerical variables"""
        numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        
        if len(numerical_cols) < 2:
            logger.info("Not enough numerical columns for pair plot")
            return
        
        # Limit to 6 columns to avoid huge plots
        if len(numerical_cols) > 6:
            numerical_cols = numerical_cols[:6]
            logger.info(f"Using first 6 numerical columns for pair plot: {numerical_cols}")
        
        try:
            # Create seaborn pair plot
            pair_plot = sns.pairplot(df[numerical_cols], diag_kind='hist', corner=False)
            pair_plot.fig.suptitle('Pair Plot of Numerical Variables', y=1.02)
            pair_plot.savefig(f'{self.visualization_path}/pair_plot.png', 
                            bbox_inches='tight', dpi=300)
            plt.close()
            logger.debug("Pair plot created")
            
        except Exception as e:
            logger.warning(f"Could not create pair plot: {str(e)}")
            
            # Fallback: create smaller pair plot with matplotlib
            try:
                self._create_simple_pair_plot(df, numerical_cols)
            except Exception as e2:
                logger.warning(f"Could not create simple pair plot: {str(e2)}")
    
    def _create_simple_pair_plot(self, df: pd.DataFrame, numerical_cols: list):
        """Create a simpler pair plot using matplotlib"""
        n_cols = len(numerical_cols)
        fig, axes = plt.subplots(n_cols, n_cols, figsize=(15, 15))
        
        for i, col1 in enumerate(numerical_cols):
            for j, col2 in enumerate(numerical_cols):
                ax = axes[i, j]
                
                if i == j:
                    # Diagonal: histogram
                    ax.hist(df[col1].dropna(), bins=20, alpha=0.7)
                    ax.set_title(f'Dist: {col1}')
                else:
                    # Off-diagonal: scatter plot
                    ax.scatter(df[col1], df[col2], alpha=0.6, s=10)
                    ax.set_xlabel(col1)
                    ax.set_ylabel(col2)
                
                # Remove ticks for better readability
                ax.tick_params(axis='both', which='major', labelsize=8)
        
        plt.suptitle('Simple Pair Plot of Numerical Variables', fontsize=16)
        plt.tight_layout()
        plt.savefig(f'{self.visualization_path}/simple_pair_plot.png', 
                   bbox_inches='tight', dpi=300)
        plt.close()
        logger.debug("Simple pair plot created")
    
    def _create_correlation_heatmap(self, df: pd.DataFrame):
        """Create correlation heatmap for numerical columns"""
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        
        if len(numerical_cols) > 1:
            try:
                plt.figure(figsize=(12, 10))
                correlation_matrix = df[numerical_cols].corr()
                
                # Create mask for upper triangle
                mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))
                
                sns.heatmap(correlation_matrix, 
                           annot=True, 
                           cmap='coolwarm', 
                           center=0, 
                           fmt='.2f', 
                           linewidths=0.5,
                           mask=mask,
                           square=True)
                plt.title('Correlation Heatmap (Lower Triangle)')
                plt.tight_layout()
                plt.savefig(f'{self.visualization_path}/correlation_heatmap.png', 
                           bbox_inches='tight', dpi=300)
                plt.close()
                logger.debug("Correlation heatmap created")
                
                # Create clustered heatmap
                self._create_clustered_heatmap(df, numerical_cols, correlation_matrix)
                
            except Exception as e:
                logger.warning(f"Could not create correlation heatmap: {str(e)}")
    
    def _create_clustered_heatmap(self, df: pd.DataFrame, numerical_cols: list, correlation_matrix: pd.DataFrame):
        """Create clustered correlation heatmap"""
        try:
            # Perform hierarchical clustering
            from scipy.cluster import hierarchy
            
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
            
            # Dendrogram
            Z = hierarchy.linkage(correlation_matrix, 'ward')
            dendrogram = hierarchy.dendrogram(Z, labels=numerical_cols, ax=ax1)
            ax1.set_title('Hierarchical Clustering Dendrogram')
            ax1.tick_params(axis='x', rotation=45)
            
            # Reorder correlation matrix based on clustering
            order = dendrogram['leaves']
            reordered_corr = correlation_matrix.iloc[order, order]
            
            # Clustered heatmap
            sns.heatmap(reordered_corr, 
                       annot=True, 
                       cmap='coolwarm', 
                       center=0, 
                       fmt='.2f', 
                       linewidths=0.5,
                       square=True,
                       ax=ax2)
            ax2.set_title('Clustered Correlation Heatmap')
            
            plt.tight_layout()
            plt.savefig(f'{self.visualization_path}/clustered_correlation_heatmap.png', 
                       bbox_inches='tight', dpi=300)
            plt.close()
            logger.debug("Clustered correlation heatmap created")
            
        except Exception as e:
            logger.warning(f"Could not create clustered heatmap: {str(e)}")
    
    def _create_boxplots(self, df: pd.DataFrame):
        """Create boxplots for numerical columns"""
        numerical_cols = df.select_dtypes(include=[np.number]).columns
        
        if len(numerical_cols) > 0:
            try:
                n_cols = min(3, len(numerical_cols))
                n_rows = (len(numerical_cols) + n_cols - 1) // n_cols
                
                fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 5 * n_rows))
                if n_rows == 1:
                    axes = [axes] if n_cols == 1 else axes
                else:
                    axes = axes.flatten()
                
                for i, col in enumerate(numerical_cols):
                    if i < len(axes):
                        df.boxplot(column=col, ax=axes[i])
                        axes[i].set_title(f'Boxplot of {col}')
                
                # Remove empty subplots
                for i in range(len(numerical_cols), len(axes)):
                    fig.delaxes(axes[i])
                
                plt.tight_layout()
                plt.savefig(f'{self.visualization_path}/boxplots.png', 
                           bbox_inches='tight', dpi=300)
                plt.close()
                logger.debug("Boxplots created")
            except Exception as e:
                logger.warning(f"Could not create boxplots: {str(e)}")