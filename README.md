# citibikeDataScrape
## What:
Python script to crawl NYC Citibike's trip data, do a quick rough clean, and push to S3 for load into Redshift for analysis.

## Why:
I created this because the stations near me are usually empty in the morning, even though I have some good trains within a 5 min walk from here. I wanted to see what the ebb and flow was for my closest stations, though I probably won't post that data here since I don't want the entire internet to be able to triangulate where my apartment is. :-)

## Backlog/Future:
* Some abstractions. Separate helper/decorator functions into helper class? Move class to separate file.
* I have an algo I'm working on to infer the inventory at any given station with the +1 -1 of started and stopped trips, however Citibike rebalances the inventory overnight sometimes and that restocking doesn't show in the publicly accessible data. I can likely exclude that via matching the bikeID for rides starting at a given station to a recent ride ending at that station, though I've put that on hold for a minute since these calcs are kind of just for fun and I'm working on some other things.
* It'd be fun once I have that historic inventory data to create a statistical recommendation engine using that takes where I am and predicts what nearby station would have the best chance of catching a bike, given a specific travel time. I.e. I'm at work in Manhattan and I want to grab a bike at 7, so what station is my best shot? More of a future-trip planning exercise, as the mobile app has real-time data if you're not planning ahead. Could implement FB's Prophet or another ML algo.
