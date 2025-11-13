# FLASHES2.0

The follow-up of the **FL**exible **A**lert **S**ystem for **H**igh **E**nergy **S**ources (FLASHES) from the European Space Agency. The current version is hosted [on this website](https://integral.esac.esa.int/flashes/). This new version will fix some well-known bugs, add frontend and backend improvements and introduce some functionalities, that are described below.

## What is FLASHES?

FLASHES is a monitoring and analyses tool for high-energy (X-ray) sources in space. Light-curve data from the Monitor of All-sky X-ray Image (MAXI), the Burst Array Telescope (BAT) of Swift, and the Gamma-Ray Burst Monitor (GBM) on Fermi are automatically downloaded, processed and analysed every day. For each source in the FLASHES source catalog, a relevance is calculated. If a worth-mentioning event is happening, a high relevance value is assigned. If nothing happens, a low relevance value is assigned. Currently, the relevance values are between 0 and 1.

The relevance calculation is based on a linear combination of a flux term, a trend term, and a hardness-change term. The flux term is proportional to the signal-to-noise ratio of the new data point. The trend term is large for a large flux trend within the last 5 data points. The hardness-change term is 1 if a hardness-change (hard X-ray flux / soft X-ray flux) occurs and 0 else. FLASHES downloads the new data from the corresponding telescope websites and subsequently runs the relevance calculation.

The sources in the FLASHES catalogue are departed into several categories. Each category has an overview table that can be assessed in the frontend. The tables provide an overview of the measurements for each source. Additionally, each source has a detail page showing all measurements, the relevance terms and additinoal details. The plots are generated with Python as interactive websites from which data can be selected and downloaded.

## FLASHES2.0 usage

FLASHES2.0 is currently in the open beta phase. This means that not all features has fully been implemented yet. This Section summarizes all available features and gives an overview of the frontend.

### Landing Page

The FLASHES2.0 landing page gives a brief description of FLASHES' functionalities, the data, sources and categorization. Additionally, an image of the catalogue is shown. The navigation bar leads to a table in which all sources are shown, a list with all available tags and to a description page. On the bottom of the landing page, the four most important tags ("Binary", "Black Hole", "Burster" and "Binary") are linked.

### Sources Lists

Several lists with sources are available. The tables show the source name, the last flux values from available telescopes, the tags and a link to the Grafana dashboards. By clicking on the source names, the detail page from the source can be accessed. Clicking on a tag leads to the source list of the tag. The tables can be sorted by name and individual sources can be searched in the search bar above.

All sources are listed under /sources. This page can be added via the "Sources" button in the navigation bar. All sources with a specific tag are listed under /tags/"tag-name".

### Tag List

All available tags are listed under /tags. The page can be accessed via the "Tags" button in the navigation bar. From this page, the source lists for tags can be accessd via the buttons.

### About Page

The about page presents more details of FLASHES. It can be accessed under /about and via the "About" button in the navigation bar.

### Source Detail Page

By clicking on the source name in the source tables, the detail pages can be accessed. This page shows all available information. Below the title, an image with the galactic position of the source is shown. Below that, the tags, and links to the [SIMBAD Astronomical Database](https://simbad.u-strasbg.fr/simbad/), the [IPAC Extragalactic Database](https://ned.ipac.caltech.edu) and the High Energy Astrophysics Science Research Center [(HEARSAC)](https://heasarc.gsfc.nasa.gov) are shown. It is noted that not all sources are available in all databases and the links can lead to non-existing pages. Below that, an overview on the last available data is given, together with the galactic coordinates. In the last part, the telescope cards can be seen, where the original data pages are linked, the data of the last week is shown (if no new data from the last week is available, empty boards are shown), and the full dataset can be downloaded as CSV.

### Grafana Dashboard

The Grafana dashboards are the main source for data visualization. They offer an overview plot showing the Swift/BAT 15-50 keV channel, the MAXI 2-20 keV channel and the Fermi/GBM 12-50 keV channel (depending on the availablity). Lightcurve visibility can be toggled by clicking on the correponding channel in the legend. If Swift/BAT and MAXI data are available, the Hardness-Intensity Diagram and the Combined Flux is shown in this section too. Below the overview section, further lightcurves are provided, sorted by telescope. The visualized time range can be adjusted with the time picker in the top right corner of the dashboard and by dragging a selection field over the time series. Via the panel links (next to the panel title), the shown data can be downloaded. Errors are visualized as shaded areas. Exact flux values are shown in the tooltip by hovering over the flux time series. If no new data is available for longer times (currently > 3 days), the plot is intersected, otherwise the lines are connected, even with one or two days without an update.

## FLASHES2.0 architecture

The architecture of this project consists of two databases, a backend for the API, data download and processing and the relevance calculation, and two frontend technologies, one for browsing and quick information and one for the dasboards. The whole project is fully containerized and designed in a way that allows for a quick local installation. In this section, an overview is given over the individual parts. Even though being only preliminary, it is aimed to fix the framework selection as soon as possible.

### Deployment

The deployment of FLASHES2.0 is done in Docker, as it allows for an easy local installation. For development, Docker v28.0.1 is used. The services are arranged in a yml-file with Docker compose v2.33.1. The inter-container communication is done in a dedicated Docker network. All secrets are stored in an env-file. If you want to set up FLASHES2.0 for yourself, an example for an env-file is provided as env-example. Please fill in the empty tokens and passwords and rename the file to .env before you start. For detailed instructions, please refer to the deployment guide below.

### Database architecture

The database system contains a service for the source information and a dedicated timeseries database for all timeseries. The source information database is a mongoDB (image version 6.0), as this is currently the latest version with long-term support. As a document-based database, mongoDB allows for excellent compability with backend technologies at sufficient speed. Each document resembles a source from the FLASHES source catalog. For each source, the following entries *must* be provided:


| Key             | Unit          | Description                                                                                                                                                                                   |
| :---------------- | :-------------- | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| _id             | str           | The source name without whitespaces and in lower letters. This is the primary key and used for identification in the backend                                                                  |
| integral_name   | str           | The source name from the Integral source catalog                                                                                                                                              |
| coord_ra        | degree        | Right Ascension coordinate of the source                                                                                                                                                      |
| coord_dec       | degree        | Declination coordinate of the source                                                                                                                                                          |
| labels_constant | list[str]     | List containing the constant tags. The tags define the categories in the frontend and which algorithm is used for the relevance calculation in the backend                                    |
| labels_dynamic  | list[str]     | List containing temporary labels. The temporary labels - also called alerts - will be written and deleted dynamically if certain events are detected.                                         |
| maxi            | Object / null | Shows whether data from MAXI is available. If so, the field contains a dictionary with the necessary information. If no MAXI data is available for this source, this field is null.           |
| swift           | Object / null | Shows whether data from Swift/BAT is available. If so, the field contains a dictionary with the necessary information. If no Swift/BAT data is available for this source, this field is null. |
| fermi           | Object / null | Shows whether data from Fermi/GBM is available. If so, the field contains a dictionary with the necessary information. If no Fermi/GBM data is available for this source, this field is null. |
| hardness_ratio  | Object / null | Provides the information necessary if hardness data is calculated. If no hardness data is calculated, this field is null.                                                                     |
| combined        | Object / null | Provides the information necessary if combined-flux data is calculated. If no combined-flux data is calculated, this field is null.                                                           |

If the fields maxi, swift or fermi are not null, the document provides the necessary information to download the new lightcurve data. It is assumed that the lightcurve data is provided chronologically ordered by the data provider. The information is the following


| Key            | Unit | Description                                                                                                                                                                                                                               |
| :--------------- | :----- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| data_url       | str  | Contains the exact url from which the data can be downloaded. If the data is calculated from existing data (in the ase of hardness or combined flux), this field does not exist.                                                          |
| influx_key     | str  | Contains an unique identifier for the influxdb, which is the telescope name, an underscore and the integral name without whitespaces. As it can be guaranteed that sources are not doubled in FLASHES, the identifiers are indeed unique. |
| last_timestamp | int  | Contains the timestamp of the latest datapoint in the timeseries. All data above this timestamp will be imported.                                                                                                                         |
| last_flux      | int  | Contains the last flux value from the InfluxDB. This field only exists for the Swift/BAT, Fermi/GBM and MAXI objects (in the case of MAXI, the 2-20 keV flux is saved).                                                                   |
| last_error     | int  | Contains the last flux-error value from the InfluxDB. This field only exists for the Swift/BAT, Fermi/GBM and MAXI objects (in the case of MAXI, the 2-20 keV flux is saved).                                                             |

If large amounts of timeseries data is stored in dictionaries, such as in a mongoDB, both memory efficiency and access speed suffer. This is why it was decided to save the timeseries data for each source in a dedicated software. A standard software package for that is InfluxDB, which is also used in this project (image version 2.7). The bucket, in which the lightcurve data is saved, is called flashes_data. Each lightcurve is tagged with a source name, either **maxi**, **swift** or **fermi**. The following table summarizes all possible data fields.


| Key                | Backend Channel-ID | Telescope        | Availability                                            |
| :------------------- | :------------------- | :----------------- | :-------------------------------------------------------- |
| flux (15-50 keV)  | 15-50             | Swift/BAT        | Only if Swift/BAT data exists for this source.          |
| error (15-50 keV) | 15-50             | Swift/BAT        | Only if Swift/BAT data exists for this source.          |
| flux (2-20 keV)    | 2-20               | MAXI             | Only if MAXI data exists for this source.               |
| error (2-20 keV)   | 2-20               | MAXI             | Only if MAXI data exists for this source.               |
| flux (2-4 keV)     | 2-4                | MAXI             | Only if MAXI data exists for this source.               |
| error (2-4 keV)    | 2-4                | MAXI             | Only if MAXI data exists for this source.               |
| flux (4-10 keV)    | 4-10               | MAXI             | Only if MAXI data exists for this source.               |
| error (4-10 keV)   | 4-10               | MAXI             | Only if MAXI data exists for this source.               |
| flux (10-20 keV)   | 10-20              | MAXI             | Only if MAXI data exists for this source.               |
| error (10-20 keV)  | 10-20              | MAXI             | Only if MAXI data exists for this source.               |
| flux (12-50 keV)   | 12-50              | Fermi/GBM        | Only if Fermi/GBM data exists for this source.          |
| error (12-50 keV)  | 12-50              | Fermi/GBM        | Only if Fermi/GBM data exists for this source.          |
| hardness ratio     | None               | MAXI & Swift/BAT | Only if MAXI and Swift/BAT data exists for this source. |
| hardness error     | None               | MAXI & Swift/BAT | Only if MAXI and Swift/BAT data exists for this source. |
| combined flux      | None               | MAXI & Swift/BAT | Only if MAXI and Swift/BAT data exists for this source. |
| combined error     | None               | MAXI & Swift/BAT | Only if MAXI and Swift/BAT data exists for this source. |

The x-ray hardness $h$ and combined x-ray fluxes $\Phi_c$ are calculated from Swift/BAT 15-50 keV data and MAXI 2-20 keV data, following these equations:

$$
h = \frac{\Phi_{15-50\text{ keV}}}{\Phi_{2-20\text{ keV}}}; \quad \Delta h = \frac{\Phi_{15-50\text{ keV}}}{\Phi_{2-20\text{ keV}}}\sqrt{\left(\frac{\Delta\Phi_{15-50\text{ keV}}}{\Phi_{15-50\text{ keV}}}\right)^2 + \left(\frac{\Delta\Phi_{2-20\text{ keV}}}{\Phi_{2-20\text{ keV}}}\right)^2}

$$

$$
\Phi_c = \Phi_{15-50\text{ keV}} + \Phi_{2-20\text{ keV}}; \quad \Delta\Phi_c = \sqrt{\left(\Delta\Phi_{15-50\text{ keV}}\right)^2 + \left(\Delta\Phi_{2-20\text{ keV}}\right)^2}

$$

An outer join of the two dataframes is done to ensure that the times of observations match for the calculations.

The mongoDB listens on port 27017 whereas the InfluxDB listens on port 8086.

### Backend

The backend offers a standardized way to access data from the database. It is structured into two areas. The first area is responsible for data download, processing and upload into the database. The second area is responsible for offering endpoints for access within the software. The backend as a whole depends on a variety of Python libraries. The top-level libraries are listed in the following table with a version number and a description why this libary is needed. It is noted that these libraries internally depend on other libraries. A complete list can be found in the `requirements.txt`, which is located in the backend folder.


| Library         | Version  | Description                                                               |
| :---------------- | :--------- | :-------------------------------------------------------------------------- |
| APScheduler     | 3.11.1   | Time-sensitive tasks, such as automated data downloads                    |
| astropy         | 7.0.1    | Astronomical calculations and number-conversions                          |
| astroquery      | 0.4.11   | Source classification with Heasarc                                        |
| fastapi         | 0.115.11 | Webframework for a REST-API                                               |
| influxdb-client | 1.48.0   | Reading and Writing into the InfluxDB                                     |
| numpy           | 2.2.4    | Generic library for numerical calculations                                |
| pandas          | 2.2.3    | Data analysis and data handling                                           |
| pymongo         | 4.11.3   | Reading and Writing into the mongoDB                                      |
| python-dotenv   | 1.0.1    | Reading of environment variables, safe handling from passwords and tokens |
| requests        | 2.32.3   | HTTP requests for data download                                           |

The backend is available at port 8000.

#### Endpoints

The backend offers a variety of endpoints for the frontend and the dashboard to visualize data or to check functionalities. The following endpoints are available:

##### Generic

Generic endpoints are offered by the fastapi library and are not necessarily part of FLASHES2.0. Nevertheless, they may offer functionalities that are useful during development or use.


| Endpoint | Description                                                       |
| :--------- | :------------------------------------------------------------------ |
| /docs    | Opens the FastAPI SwaggerUI for testing and interactive API calls |

##### Health

The health endpoints are necessary to check the correct functionality of all services. If a healthcheck was successful, the status "ok" (200) is returned. Otherwise, the status "error" (503) is returned together with the exception that caused the healthcheck to fail. The table below shows the supported healthchecks, including the corresponding URL, a description and how the healthcheck is performed.


| Endpoint         | Description                    | Healthcheck                                            |
| :----------------- | :------------------------------- | -------------------------------------------------------- |
| /health/mongo    | Healthcheck for the mongoDB    | Tries to count all documents in the sources collection |
| /health/influx   | Healthcheck for the InfluxDB   | Connects to the off-the-shelf health endpoint          |
| /health/frontend | Healthcheck for the frontend   | Calls the frontend landing page                        |
| /health/grafana  | Healthcheck for the dashboards | Connects to the off-the-shelf health endpoint          |

##### Source information

The source-information endpoints are used to provide generic information of the sources in the catalog to the frontend. To identify the sources, the _id field from the mongoDB is used.


| Endpoint       | Description                                                                        |
| :--------------- | :----------------------------------------------------------------------------------- |
| /sources       | Lists all sources from the catalog with basic information as array of dictionaries |
| /sources/{_id} | Lists all available details for a source in a dictionary                           |

##### Tagged data

The tagging endpoint provides pre-filtered source lists to obey filtering on the client side. Additionally, an endpoint is provided that returns a list of all available tags. More information on tagging is provided in the corresponding section of this file.


| Endpoint    | Description                               |
| ------------- | ------------------------------------------- |
| /tags       | Lists all available tags                  |
| /tags/{tag} | Lists all sources with this specific tag. |

##### Timeseries data

The timeseries endpoints are used to connect the dashboards to the InfluxDB. The timeseries are defined by the telescope-specific influx key from the mongoDB, channel information and a timeframe.


| Endpoint    | Description                                           |
| :------------ | :------------------------------------------------------ |
| /timeseries | Lists data from influxDB in a Grafana-readable format |

It is noted that this data endpoint is to be used internally from the software only and is hence tailored to the needs of Grafana, which contains a list of dictionaries. Each dictionary provides a timestamp, the corresponding flux (named after the fields in the corresponding Table in the backend Section, the flux plus the error (naming: "flux {channel} max") and the flux minus the error (naming: "flux {channel} min"). This has the reason that the errors are displayed as shaded areas around the lightcurve rather than as error bars. To select certain data sets in the dashboard, four parameters can be handed over to the InfluxDB query:


| Parameter  | Description                          |
| :----------- | :------------------------------------- |
| influx_key | Influx ID from the MongoDB           |
| channel    | Channel information                  |
| start      | Starting time for the InfluxDB query |
| end        | Ending time for the InfluxDB query   |

The influx_key parameter has to be given in order to lead to data. Furthermore, the parameter channel has to be given if Swift/BAT, MAXI or Fermi/GBM data is accessed to select the certain channel. The time-frame information, i.e. timeStart and timeEnd are optional if data from a certain timeframe is needed. If timeStart and timeEnd are not provided, data from the last year is loaded.

##### Data download

To download data from FLASHES2.0, a system similar to the timeseries-data endpoint is used. The timeseries are defined by the influx key from the mongoDB. Additionally, generic source information can be downloaded. To identify the source, the _id field from the mongoDB is used. It is noted that these endpoints are to be used by the user to access data.


| Endpoint               | Description                                      |
| :----------------------- | :------------------------------------------------- |
| /download/{influx_key} | Downloads a timeseries defined by the influx key |

To define the start and end of the desired download, two parameters can be handed over to the InfluxDB query:


| Parameter | Description                          |
| :---------- | :------------------------------------- |
| start     | Starting time for the InfluxDB query |
| end       | Ending time for the InfluxDB query   |

When downloading data from Fermi/GBM, the column regarding the error is always 0. This is not a problem from FLASHES2.0 but rather from the supplier of the data as the fits files that are downloaded show the same behaviour.
The download for a given timerange can be triggered directly from the Grafana dashboard. In this case, the selected timerange from the dashboard will be applied. It is noted that the download endpoint does not support channels at the moment. If necessary, this can be changed in the future.

##### Plots

The plots are available via a dedicated endpoint. As not all sources support the same data sets, the source ID from the mongoDB is passed.


| Endpoint     | Description                                     |
| -------------- | ------------------------------------------------- |
| /plots/{_id} | Forwards to the corresponding Grafana dashboard |

After the call, the backend connects to the mongoDB and assesses the swift, maxi and fermi information to calculate which dashboard for the template has to be used. It then forwards to Grafana and passes the influx keys from all available data as URL query parameters.

### Frontend

FLASHES2.0 includes to user-oriented software parts. The frontend acts as a starting point for the user and offers the navigation through the application and pages for all information that are not timeseries data. The timeseries data are shown in Grafana dashboards. The frontend is based the Next.js framework and includes typescript for the logic calculations and tailwindcss for styling classes. A list of all necessary libraries are shown in the Table below.


| Library     | Version | Description                                              |
| ------------- | --------- | ---------------------------------------------------------- |
| react-dom   | 19.1.0  | Client-rendering library for React                       |
| next        | 15.5.5  | Next.js framework                                        |
| typescript  | ^5      | TypeScript compiler                                      |
| tailwindcss | ^4      | Tailwind CSS utility framework, incl. the PostCSS plugin |

### Dashboards

For each source, a dashboard is provided, showing all available lightcurve data and hardness information. In this project, Grafana (image grafana/grafana) is used to provide the dashboard environment. As not all sources support the same data sets, the dashbaords have to be created depending on the available information. The following combinations are available:


| Combination                  | Template Name                  | Dashboard UID            | Comment                                    |
| :----------------------------- | :------------------------------- | :------------------------- | :------------------------------------------- |
| MAXI                         | template-maxi.json             | flashes-maxi             |                                            |
| Swift/BAT                    | template-swift.json            | flashes-swift            |                                            |
| Swift/BAT + MAXI             | template-swift-maxi.json       | flashes-swift-maxi       | hardness ratio and combined flux available |
| Swift/BAT + Fermi/GBM        | template-swift-fermi.json      | flashes-swift-fermi      |                                            |
| Swift/BAT + MAXI + Fermi/GBM | template-swift-maxi-fermi.json | flashes-swift-maxi-fermi | hardness ratio and combined flux available |

- only MAXI data is available
- only Swift/BAT data is available
- Swift/BAT and MAXI data is available -> in this case, also combined fluxes and hardness ratio is calculated
- Swift/BAT and Fermi/GBM data is available
- Swift/BAT, MAXI and Fermi/GBM data is available -> in this case, also combined fluxes and hardness ratio is calculated

For each of the six cases, a template for a dashboard is available. In the template, the placeholder for the source name is 'integral_name'. The placeholders for the influx keys are 'swift_influxkey', 'maxi_influxkey', 'fermi_influxkey', 'hardness_influxkey' and 'combined_influxkey'. They are marked as variables as ${...} in Grafana and their usage is described [here](https://grafana.com/docs/grafana/latest/dashboards/build-dashboards/create-dashboard-url-variables/). When assessing the dashboard for a source, the placeholders are replaced with the corresponding keys from the mongoDB by forwarding them in the URL of the dashboard. The templates are available in Grafana in the template folder.

The dashboards are structured in the following way. On top, there is an overview section, which covers the most important available lightcurves (Swift/BAT 15-50 keV, MAXI 2-20 keV and Fermi/GBM 12-50 keV). If Swift and MAXI data are available, the overview section covers the Hardness-Intensity diagram and a lightcurves of the combined flux.

The connection from the dashboard to the InfluxDB happens via the backend. The Infinity Datasource is used as datasource in Grafana. The UID of the datasource is 'flashes-datasource'

## Source Tagging

To group the source catalogue into classes and to make things easier if someone is only interested in a particular class of sources, each source has at least one tag for identification. The tags are based on the [HEASARC Object Classification](https://heasarc.gsfc.nasa.gov/W3Browse/all/class.html). Following this classification, each source has a four-digit number als class ID. This ID is saved in the Master Table. From the ID, a classification can be done using the Heasarc class from the astroquery Python library. To get a common basis for the tags, all Class Names were analyzed and the most abundant and meaningfull words were selected. The available tags are (in non particular order):


| Tag              | Description                                                                                                                                                                                         |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| LMXRB            | Low-Mass X-Ray Binary - A binary system, in which a Black Hole or a Neutron Star accretes matter from a low-mass companion. The hot matter produces light in the x-ray regime.                      |
| SEYFERT GALAXY   | A subclass of AGN with a luminous nucleus. Emits x-rays though hot gas in the accretion disk.                                                                                                       |
| GLOBULAR CLUSTER | A spherically-shaped group of typically very old stars. Frequently hosting x-ray sources [Pooley, 2010]                                                                                             |
| GAMMA RAY SOURCE | Object with such extreme conditions that gamma rays are emitted.                                                                                                                                    |
| BE STAR          | B-type star with a circumstellar disk. Common donor in Be/X-ray binaries.                                                                                                                           |
| BL LAC           | A subclass of AGN, in which the jet is aligned with the line of sight. Emits x-ray though hot matter in the jet.                                                                                    |
| BLACK HOLE       | Extreme-gravity environments. Often surrounded by rotation disks of hot gas and other matter, which emit x-ray due to their high temperatures.                                                      |
| GALAXY CLUSTER   | Large-scale structures of galaxies bound together via gravity. Emit x-rays due to bremsstrahlung, recombination or deexcitation of electrons [Böhringer, 2010]                                     |
| CLUSTER          | A bound group of astrophysical objects, typically stars or galaxies, bound together via gravity. Emits x-rays typically through bremsstrahlung.                                                     |
| PULSAR           | Rapidly rotating neutron star, emitting periodic pulses across the whole electromagnetic spectrum.                                                                                                  |
| TRANSIENT        | A source that brightens/appears only temporarily. Can have a variety of reasons.                                                                                                                    |
| HMXRB            | High-Mass X-Ray Binary - A binary system, in which a Black Hole or a Neutron Star accretes matter from a high-mass companion. The hot matter produces light in the x-ray regime.                    |
| AGN              | Active Galactic Nucleus - A bright central region of a galaxy powered by accretion of matter onto a supermassive Black Hole. The matter is heated up during the accretion process and emits x-rays. |
| SUPERGIANT       | A massive star, which can emit strong x-rays when their stellar wind is accreted by another object.                                                                                                 |
| PULSATOR         | An object with strong brightness variations. Can be connected to neutron stars and/or accretion processes.                                                                                          |
| BURSTER          | An object showing very sudden increases in brightness, often due to thermonuclear processes.                                                                                                        |
| REPEATER         | An object with recurrent bursts or flares.                                                                                                                                                          |
| HARD             | Refers to the type of x-rays. Hard x-rays are dominated by high-energy photons.                                                                                                                     |
| BINARY           | Gravitationally bound system of two objects. Often lead to x-ray emission if matter from one object is accreted onto its counterpart.                                                               |
| QSO              | Quasi-Stellar Object (QUASAR) - A subclass of AGN with extremly large luminosity. Emits x-ray though hot matter in the accretion disk.                                                              |
| QPO              | Quasi-Periodic Oscillation - Nearly periodic modulations in x-ray flux with some exceptions.                                                                                                        |
| UNIDENTIFIED     | X-ray source with a known multi-wavelength counterpart.                                                                                                                                             |
| UNCLASSIFIED     | Classification not done or not possible. Default tag if no other tag is given.                                                                                                                      |

## Relevance Calculation

The relevance calculation is the key part of FLASHES that might lead to new research. It is shaped in a way that sources behaving odd, in the sense of deviating from their usual behaviour, are add touted as exceptionally relevant by FLASHES. A few examples, in which cases a source should be advertised:

- A typically rather quite x-ray source has a sudden outburst.
- A source with a regular outburst pattern deviates from this pattern either in outburst frequency or peak luminosity.
- A source switches from dominantly hard x-ray emission to dominantly soft x-ray emission or vice versa.

In all these cases, the relevance should be computed as close to 1 (very relevant). Sources behaving as suspected should have a relevance of 0 (not relevant).

As no modified physical model exists for x-ray outburst to date - which is relatable as the sources covered in the FLASHES2.0 catalogue are of very different types - the relevance calculation has to be purely data-driven. To provide a reliable relevance for every source individually, a Machine Learning Algorithm is deployed which learns the behaviour of every source, analyses the newly incoming datapoint and calculates the relevance accordingly.

## Alerting

FLASHES2.0 supports alerting, which are shown on the web-application.

## Deployment Guide

FLASHES2.0 can be hosted locally to tailor relevance calculations to individual needs. This guide shows how to set up FLASHES2.0 on your local device.

1. Clone the project to the desired directory.
2. Fill in your credentials in the env-example file and rename the file to ".env".
3. Install Docker and compose if not already happened.
4. Build all services and launch the mongoDB.
5. Run the script backend/filldb.py, which fills the mongoDB with life. You might want to create a venv from the requirements.txt first, which is located in the backend folder.
6. Once the mongoDB is filled, launch the remaining services.

Once the Scheduler in the backend is started, the first download is triggered, which downloads all available data. Depending on the host system, this can take a while (two or more hours), so be patient and take an eye on the backend logs. Once done, the updates are done every day.

## Roadmap

[🎉️] Fix framework selection

[ ] Agree on licensing

[🎉️] Fully set up the container system

[🎉️] Implement the dashboards

[ ] Implement a frontend

**--- RELEASE FLASHES2.0 beta ---**

[ ] Implement a relevance calculation

[ ] Implement alerting

**--- RELEASE v1.0 ---**

## Further Ideas

- FLASHES supported user-handling to "subscribe" to specific sources of interest. FLASHES2.0 should implement a similar feature but ideally without credential handling to keep things safe. An idea is to map generate a hashcode from the sources of interest, which then can be saved on the user-side and pasted into FLASHES2.0 to generate an individual dashboard from this code.
- FLASHES2.0 should support alerting via E-Mail for individual sources. The E-Mail adresses has to be treated with care to assure data security measures and must be revokable.

## Introduced Changes to FLASHES

### Necessary packages and software

- Docker (v28.0.1), incl. compose (v2.33.1)
- mongoDB (Docker image: mongo:6.0)
- InfluxDB (Docker image: influxdb:2.7)
- Grafana (Docker image: grafana/grafana), incl. the yesoreyeram-infinity-datasource plugin
- Python (v3.13.7), incl. libraries from the corresponding Table in the backend section
- react 19.1.0, incl. libraries from the corresponding Table in the frontend section

## References

**Böhringer [2010]:** Böhringer, H., Werner, N. X-ray spectroscopy of galaxy clusters: studying astrophysical processes in the largest celestial laboratories. *Astron Astrophys Rev 18*, 127–196 (2010) [https://doi.org/10.1007/s00159-009-0023-3](https://doi.org/10.1007/s00159-009-0023-3).

**Pooley [2010]:** D. Pooley, Globular cluster x-ray sources, *Proc. Natl. Acad. Sci. U.S.A.* 107 (16) 7164-7167 (2010), [https://doi.org/10.1073/pnas.0913903107](https://doi.org/10.1073/pnas.0913903107).
