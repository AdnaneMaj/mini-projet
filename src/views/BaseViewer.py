from helpers.config import get_settings

import os
import matplotlib.pyplot as plt
import pandas as pd

class BaseViewer:

    csv_file = os.path.join(os.path.dirname(os.path.dirname(__file__)),'assets','result.csv')

    def __init__(self):
        self.app_settings = get_settings()

    def plot_from_csv(self):

        # Load the CSV file
        data = pd.read_csv(BaseViewer.csv_file)

        # Create a bar plot
        plt.figure(figsize=(10, 6))
        plt.bar(data['year'], data['mean'], color='b', alpha=0.7)

        # Add titles and labels
        plt.title('Year vs Mean (Bar Plot)', fontsize=16)
        plt.xlabel('Year', fontsize=14)
        plt.ylabel('Mean', fontsize=14)
        plt.xticks(rotation=45)  # Rotate the x-axis labels for better readability
        plt.grid(True, linestyle='--', alpha=0.6)

        # Show the plot
        plt.show()
