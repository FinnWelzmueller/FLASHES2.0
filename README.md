# FLASHES2.0

The follow-up of the **FL**exible **A**lert **S**ystem for **H**igh **E**nergy **S**ources (FLASHES) from the European Space Agency. The current version is hosted [on this website](https://integral.esac.esa.int/flashes/). This new version will fix some well-known bugs, add frontend and backend improvements and introduce some functionalities, that are described below.

## What is FLASHES?

FLASHES is a monitoring and analyses tool for high-energy (X-ray) sources in space. Light-curve data from the Monitor of All-sky X-ray Image (MAXI), the Burst Array Telescope (BAT) of Swift, and the Gamma-Ray Burst Monitor (GBM) on Fermi are automatically downloaded, processed and analysed every day. For each source in the FLASHES source catalog, a relevance is calculated. If a worth-mentioning event is happening, a high relevance value is assigned. If nothing happens, a low relevance value is assigned. Currently, the relevance values are between 0 and 1.

The relevance calculation is based on a linear combination of a flux term, a trend term, and a hardness-change term. The flux term is proportional to the signal-to-noise ratio of the new data point. The trend term is large for a large flux trend within the last 5 data points. The hardness-change term is 1 if a hardness-change (hard X-ray flux / soft X-ray flux) occurs and 0 else. FLASHES downloads the new data from the corresponding telescope websites and subsequently runs the relevance calculation.

The sources in the FLASHES catalogue are departed into several categories. Each category has an overview table that can be assessed in the frontend. The tables provide an overview of the measurements for each source. Additionally, each source has a detail page showing all measurements, the relevance terms and additinoal details. The plots are generated with Python as interactive websites from which data can be selected and downloaded.

## FLASHES2.0 architecture

The architecture of this project consists of two databases, a backend for the API, data download and processing and the relevance calculation, and two frontend technologies, one for browsing and quick information and one for the dasboards. The whole project is fully containerized and designed in a way that allows for a quick local installation. In this section, an overview is given over the individual parts. Even though being only preliminary, it is aimed to fix the framework selection as soon as possible.

### Deployment

The deployment of FLASHES2.0 is done in Docker, as it allows for an easy local installation. For development, Docker v28.0.1 is used. The services are arranged in a yml-file with Docker compose v2.33.1. The inter-container communication is done in a dedicated Docker network. All secrets are stored in an env-file. If you want to set up FLASHES2.0 for yourself, an example for an env-file is provided as env-example. Please fill in the empty tokens and passwords and rename the file to .env before you start. For detailed instructions, please refer to the deployment guide below.

### Database architecture

The database system contains a service for the source information and a dedicated timeseries database for all timeseries. The source information database is a mongoDB (image version 6.0), as this is currently the latest version with long-term support. As a document-based database, mongoDB allows for excellent compability with backend technologies at sufficient speed. Each document resembles a source from the FLASHES source catalog. For each source, the following entries *must* be provided:


|       Key       |     Unit     | Description                                                                                                                                                                                   |
| :---------------: | :-------------: | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|       _id       |      str      | The source name without whitespaces and in lower letters. This is the primary key and used for identification in the backend                                                                  |
|  integral_name  |      str      | The source name from the Integral source catalog                                                                                                                                              |
|    coord_ra    |    degree    | Right Ascension coordinate of the source                                                                                                                                                      |
|    coord_dec    |    degree    | Declination coordinate of the source                                                                                                                                                          |
| labels_constant |   list[str]   | List containing the constant tags. The tags define the categories in the frontend and which algorithm is used for the relevance calculation in the backend                                    |
| labels_dynamic |   list[str]   | List containing temporary labels. The temporary labels - also called alerts - will be written and deleted dynamically if certain events are detected.                                         |
|      maxi      | Object / null | Shows whether data from MAXI is available. If so, the field contains a dictionary with the necessary information. If no MAXI data is available for this source, this field is null.           |
|      swift      | Object / null | Shows whether data from Swift/BAT is available. If so, the field contains a dictionary with the necessary information. If no Swift/BAT data is available for this source, this field is null. |
|      fermi      | Object / null | Shows whether data from Fermi/GBM is available. If so, the field contains a dictionary with the necessary information. If no Fermi/GBM data is available for this source, this field is null. |
| hardness_ratio | Object / null | Provides the information necessary if hardness data is calculated. If no hardness data is calculated, this field is null.                                                                     |
|    combined    | Object / null | Provides the information necessary if combined-flux data is calculated. If no combined-flux data is calculated, this field is null.                                                           |

If the fields maxi, swift or fermi are not null, the document provides the necessary information to download the new lightcurve data. The information is the following


|      Key      | Unit | Description                                                                                                                                                                                                                               |
| :--------------: | :----: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|    data_url    | str | Contains the exact url from which the data can be downloaded. If the data is calculated from existing data (in the ase of hardness or combined flux), this field does not exist.                                                          |
|   influx_key   | str | Contains an unique identifier for the influxdb, which is the telescope name, an underscore and the integral name without whitespaces. As it can be guaranteed that sources are not doubled in FLASHES, the identifiers are indeed unique. |
| last_timestamp | int | Contains the timestamp of the latest datapoint in the timeseries. All data above this timestamp will be imported. This technique assumes that the lightcurve data is provided chronologically ordered.                                    |

If large amounts of timeseries data is stored in dictionaries, such as in a mongoDB, both memory efficiency and access speed suffer. This is why it was decided to save the timeseries data for each source in a dedicated software. A standard software package for that is InfluxDB, which is also used in this project (image version 2.7). The bucket, in which the lightcurve data is saved, is called flashes_data. Each lightcurve is tagged with a source name, either **maxi**, **swift** or **fermi**. The following table summarizes all possible data fields.


|        Key        | Backend Channel-ID |    Telescope    | Availability                                            |
| :------------------: | -------------------- | :----------------: | --------------------------------------------------------- |
| flux (15-150 keV) | 15-150             |    Swift/BAT    | Only if Swift/BAT data exists for this source.          |
| error (15-150 keV) | 15-150             |    Swift/BAT    | Only if Swift/BAT data exists for this source.          |
|  flux (2-20 keV)  | 2-20               |       MAXI       | Only if MAXI data exists for this source.               |
|  error (2-20 keV)  | 2-20               |       MAXI       | Only if MAXI data exists for this source.               |
|   flux (2-4 keV)   | 2-4                |       MAXI       | Only if MAXI data exists for this source.               |
|  error (2-4 keV)  | 2-4                |       MAXI       | Only if MAXI data exists for this source.               |
|  flux (4-10 keV)  | 4-10               |       MAXI       | Only if MAXI data exists for this source.               |
|  error (4-10 keV)  | 4-10               |       MAXI       | Only if MAXI data exists for this source.               |
|  flux (10-20 keV)  | 10-20              |       MAXI       | Only if MAXI data exists for this source.               |
| error (10-20 keV) | 10-20              |       MAXI       | Only if MAXI data exists for this source.               |
|  flux (12-50 keV)  | 12-50              |    Fermi/GBM    | Only if Fermi/GBM data exists for this source.          |
| error (12-50 keV) | 12-50              |    Fermi/GBM    | Only if Fermi/GBM data exists for this source.          |
|   hardness ratio   | None               | MAXI & Swift/BAT | Only if MAXI and Swift/BAT data exists for this source. |
|   hardness error   | None               | MAXI & Swift/BAT | Only if MAXI and Swift/BAT data exists for this source. |
|   combined flux   | None               | MAXI & Swift/BAT | Only if MAXI and Swift/BAT data exists for this source. |
|   combined error   | None               | MAXI & Swift/BAT | Only if MAXI and Swift/BAT data exists for this source. |

The x-ray hardness $h$ and combined x-ray fluxes $\Phi_c$ are calculated from Swift/BAT 15-150 keV data and MAXI 2-20 keV data, following these equations:

$$
h = \frac{\Phi_{15-150\text{ keV}}}{\Phi_{2-20\text{ keV}}}; \quad \Delta h = \frac{\Phi_{15-150\text{ keV}}}{\Phi_{2-20\text{ keV}}}\sqrt{\left(\frac{\Delta\Phi_{15-150\text{ keV}}}{\Phi_{15-150\text{ keV}}}\right)^2 + \left(\frac{\Delta\Phi_{2-20\text{ keV}}}{\Phi_{2-20\text{ keV}}}\right)^2}

$$

$$
\Phi_c = \Phi_{15-150\text{ keV}} + \Phi_{2-20\text{ keV}}; \quad \Delta\Phi_c = \sqrt{\left(\Delta\Phi_{15-150\text{ keV}}\right)^2 + \left(\Delta\Phi_{2-20\text{ keV}}\right)^2}

$$

An outer join of the two dataframes is done to ensure that the times of observations match for the calculations.

### Backend

The backend offers a standardized way to access data from the database. It is structured into two areas. The first area is responsible for data download, processing and upload into the database. The second area is responsible for offering endpoints for access within the software. The backend as a whole depends on a variety of Python libraries. The top-level libraries are listed in the following table with a version number and a description why this libary is needed. It is noted that these libraries internally depend on other libraries. A complete list can be found in the `requirements.txt`, which is located in the backend folder.


|     Library     | Version | Description                                                               |
| :---------------: | :--------: | :-------------------------------------------------------------------------- |
|   APScheduler   |  3.11.1  | Time-sensitive tasks, such as automated data downloads                    |
|     astropy     |  7.0.1  | Astronomical calculations and number-conversions                          |
|   astroquery   |  0.4.11  | Source classification with Heasarc                                        |
|     fastapi     | 0.115.11 | Webframework for a REST-API                                               |
| influxdb-client |  1.48.0  | Reading and Writing into the InfluxDB                                     |
|      numpy      |  2.2.4  | Generic library for numerical calculations                                |
|     pandas     |  2.2.3  | Data analysis and data handling                                           |
|     pymongo     |  4.11.3  | Reading and Writing into the mongoDB                                      |
|  python-dotenv  |  1.0.1  | Reading of environment variables, safe handling from passwords and tokens |
|    requests    |  2.32.3  | HTTP requests for data download                                           |

#### Endpoints

The backend offers a variety of endpoints for the frontend and the dashboard to visualize data or to check functionalities. The following endpoints are available:

##### Generic

Generic endpoints are offered by the fastapi library and are not necessarily part of FLASHES2.0. Nevertheless, they may offer functionalities that are useful during development or use.


| Endpoint | Description                                                       |
| ---------- | ------------------------------------------------------------------- |
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


| Endpoint       | Description                                               |
| :--------------- | :---------------------------------------------------------- |
| /sources       | Lists all sources from the catalog with basic information |
| /sources/{_id} | Lists all available details for a source                  |

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
| timeStart  | Starting time for the InfluxDB query |
| timeEnd    | Ending time for the InfluxDB query   |

The influx_key parameter has to be given in order to lead to data. Furthermore, the parameter channel has to be given if Swift/BAT, MAXI or Fermi/GBM data is accessed to select the certain channel. The time-frame information, i.e. timeStart and timeEnd are optional if data from a certain timeframe is needed. If timeStart and timeEnd are not provided, data from the last year is loaded.

##### Data download

To download data from FLASHES2.0, a system similar to the timeseries-data endpoint is used. The timeseries are defined by the influx key from the mongoDB. Additionally, generic source information can be downloaded. To identify the source, the _id field from the mongoDB is used. It is noted that these endpoints are to be used by the user to access data.


| Endpoint               | Description                                      |
| :----------------------- | :------------------------------------------------- |
| /download/{influx_key} | Downloads a timeseries defined by the influx key |

To define the start and end of the desired download, two parameters can be handed over to the InfluxDB query:


| Parameter | Description                          |
| :---------- | :------------------------------------- |
| timeStart | Starting time for the InfluxDB query |
| timeEnd   | Ending time for the InfluxDB query   |

### Frontend

### Dashboards

For each source, a dashboard is provided, showing all available information. In this project, grafana (image grafana/grafana) is used to provide the dashboard environment‚

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

## Deployment Guide

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

[ ] Fully set up the container system

[ ] Implement the dashboards

[ ] Implement a frontend

[ ] Implement a relevance calculation

## Further Ideas

## Introduced Changes to FLASHES

### Necessary packages and software

- Docker (v28.0.1), incl. compose (v2.33.1)
- mongoDB (Docker image: mongo:6.0)
- InfluxDB (Docker image: influxdb:2.7)
- Grafana (Docker image: grafana/grafana)
- Python (v3.13.7), incl. libraries from the corresponding Table in the backend section

## References

**Böhringer [2010]:** Böhringer, H., Werner, N. X-ray spectroscopy of galaxy clusters: studying astrophysical processes in the largest celestial laboratories. *Astron Astrophys Rev 18*, 127–196 (2010) [https://doi.org/10.1007/s00159-009-0023-3](https://doi.org/10.1007/s00159-009-0023-3).

**Pooley [2010]:** D. Pooley, Globular cluster x-ray sources, *Proc. Natl. Acad. Sci. U.S.A.* 107 (16) 7164-7167 (2010), [https://doi.org/10.1073/pnas.0913903107](https://doi.org/10.1073/pnas.0913903107).
