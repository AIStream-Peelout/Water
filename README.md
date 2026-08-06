# Water
[![CircleCI](https://dl.circleci.com/status-badge/img/gh/AIStream-Peelout/Water/tree/master.svg?style=svg)](https://dl.circleci.com/status-badge/redirect/gh/AIStream-Peelout/Water/tree/master)

Water is the hydrology-specific application and research repository for collecting, preparing,
visualizing, and analyzing United States water-resource data. It owns hydrology datasets and
experiments, but it is not a general machine-learning framework. Hydrology forecasting experiments
use [Flow Forecast](https://github.com/AIStream-Peelout/flow-forecast) for reusable models,
training, evaluation, plotting, inference, and sweep infrastructure. General framework fixes belong
in Flow Forecast; Water should consume them rather than reimplement them. See
[AGENTS.md](AGENTS.md) for the repository boundary.

This repository also contains notebooks used to build river flow data around the U.S. for FlowDB 1.0 research at [NIPS 2020 Cimate Change Workshop](https://arxiv.org/abs/2012.11154).

09/01: Updating Repository for [FlowDB 2.0](https://flow-forecast.atlassian.net/wiki/spaces/FF/pages/1178501121/FlowDB+2.0) See Confluence.

12/06  Functions for scraper object in [Python code](https://github.com/AIStream-Peelout/Water/blob/master/scraping_functions.py) now available.

2/18 End-to-End Scrape using Sentinel and SNOTEL working.






This repository is dedicated in memory of Sammy. He was the sweetest pup and was taken way too soon. If you use found this code helpful 
please consider donating to th [Dumb Friends League](https://www.ddfl.org).
