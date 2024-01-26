# FlowDB 2.0 Notebooks

These notebooks are used to buld the [FlowDB 2.0 dataset](https://flow-forecast.atlassian.net/wiki/spaces/FF/pages/1178501121/FlowDB+2.0)

- `simplfied_scraper.ipynb` is a simplfied version of `build_dataset.ipynb` used to construct data for a single river.
- `Add_SNOTEL_STAT_WESTERN.ipynb` is a notebook with exploratory code of adding SNOTEL data + initial code for scraping data. 
- `ETL_TRANSFER_ADD_SNOTEL_META.ipynb` is the pipeline we used to add the SNOTEL meta-data and transfer meta-data to new project.
- `Adding_GCP_Sentinel_Meta_Basin.ipynb` is the notebook where there is code to add the Sentinel-2 Tile ID to the gage basin meta-data.
- `ADD_SCAN_TO_META.ipynb` is the notebook where we add the SCAN site information (distances between SCAN sites and USGS gage).$$
