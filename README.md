# LCT
La Croix Tracker (LCT) is a web application designed to quantitatively visualize and analyze my partner's La Croix consumption habits.

# Features
* **Visualize Trends**: View graphical presentations of consumptions patterns and statistical data related to La Croix consumption, such as preferred flavors, average "finishing" rate (of cans), average price spent, preferred stores, and more.
* **Interactive User Section**: Participate in a user interactive section where viewers can submit ideas for data tracking or engage in bets/predictions for various statistics (i.e. the most drunk flavor of the month or how fast a particular flavor will be drunk)
*  **Data Analysis**: View various statistical analyses in addition to graphical representations

# Usage
1. Navigate to website domain
2. View graphs or statistics on respective pages
3. Submit analytical requests or predictions on respective pages

# Code and Resources Used
* Django
* Python
* HTML/CSS/JS
* Pandas
* MatPlotLib
* NumPy

# Data
## Acquisition
- For each box overall, the following is noted: purchase date, price, and location; when the first can was started and when the last was finished; flavor
- Within each box and prior to being started, each can is weighed in grams and fluid ounces. Once they are deemed finished, the final weights are collected and theyre categorized as empty, not finished, or slightly finished. 

## Basic Analysis
- Purchase location is tracked to highlight any stores patterns regarding preferences, whether certain flavors come primarily from certain locations
- Purchase date is tracked to analyze purchases over time (per month/quarter/year)
- Price is tracked for spending analysis, can be coupled with location data for price per location or coupled with percent loss to determine wasted spending, etc
- Box start and end dates are tracked for general *drink velocity* (DV) analysis (how fast a box is drunk over time); can be coupled with flavors to see DV differences between flavors
- Based on differences in final and initial can weights, the percent loss is calculated and overall averaged per box; allows determination of personal flavor preferences or if coupled with purchase date, could be explained by whatever was happening in life at that time (i.e. super busy during finals resulting in decreased (or increased) DV)
- Determining whether cans are slightly finished is important for collecting true empty can weights which allows for an average empty can weight to isolate the weight of only the liquid

# License
La Croix Tracking (LCT) is licensed under the GNU General Public License v3.0. You are free to use, modify, and distribute the code in accordance with the terms specified in the license.

# Contact
For questions, suggestions, or any inquiries about the project, you can contact me at therrymalone@gmail.com.
