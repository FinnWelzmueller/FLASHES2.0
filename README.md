# FLASHES2.0

The follow-up of the **FL**exible **A**lert **S**ystem for **H**igh **E**nergy **S**ources (FLASHES) from the European Space Agency. The current version is hosted [on this website](https://integral.esac.esa.int/flashes/). This new version will fix some well-known bugs, add frontend and backend improvements and introduce some functionalities, that are described below.

## What is FLASHES?

FLASHES is a monitoring and analyses tool for high-energy (X-ray) sources in space. Light-curve data from the Monitor of All-sky X-ray Image (MAXI), the Burst Array Telescope (BAT) of Swift, and the Gamma-Ray Burst Monitor (GBM) on Fermi are automatically downloaded, processed and analysed every day. For each source in the FLASHES source catalog, a relevance is calculated. If a worth-mentioning event is happening, a high relevance value is assigned. If nothing happens, a low relevance value is assigned. Currently, the relevance values are between 0 and 1.

The relevance calculation is based on a linear combination of a flux term, a trend term, and a hardness-change term. The flux term is proportional to the signal-to-noise ratio of the new data point. The trend term is large for a large flux trend within the last 5 data points. The hardness-change term is 1 if a hardness-change (hard X-ray flux / soft X-ray flux) occurs and 0 else. FLASHES downloads the new data from the corresponding telescope websites and subsequently runs the relevance calculation.

The sources in the FLASHES catalogue are departed into several categories. Each category has an overview table that can be assessed in the frontend. The tables provide an overview of the measurements for each source. Additionally, each source has a detail page showing all measurements, the relevance terms and additinoal details. The plots are generated with Python as interactive websites from which data can be selected and downloaded.

## FLASHES2.0 architecture

The architecture of this project consists of two databases, a backend for the API, data download and processing and the relevance calculationm, and two frontend technologies, one for browsing and quick information and one for the dasboards. The whole project is fully containerized and designed in a way that allows for a quick local installation. In this section, an overview is given over the individual parts. Even though being only preliminary, it is aimed to fix the framework selection as soon as possible.

### Deployment

The deployment of FLASHES2.0 is done in Docker, as it allows for an easy local installation. For development, Docker v28.0.1 is used. The services are arranged in a yml-file with Docker compose v2.33.1. The inter-container communication is done in a dedicated Docker network. All secrets are stored in an env-file.

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


|        Key        |  Cor. Telescope  | Comment                                                          |
| :------------------: | :----------------: | ------------------------------------------------------------------ |
| flux (15-150 keV) |    Swift/BAT    |                                                                  |
| error (15-150 keV) |    Swift/BAT    |                                                                  |
|  flux (2-20 keV)  |       MAXI       |                                                                  |
|  error (2-20 keV)  |       MAXI       |                                                                  |
|   flux (2-4 keV)   |       MAXI       |                                                                  |
|  error (2-4 keV)  |       MAXI       |                                                                  |
|  flux (4-10 keV)  |       MAXI       |                                                                  |
|  error (4-10 keV)  |       MAXI       |                                                                  |
|  flux (10-20 keV)  |       MAXI       |                                                                  |
| error (10-20 keV) |       MAXI       |                                                                  |
|  flux (12-50 keV)  |    Fermi/GBM    |                                                                  |
| error (12-50 keV) |    Fermi/GBM    |                                                                  |
|   hardness ratio   | MAXI & Swift/BAT | Only available if MAXI and Swift/BAT data exists for this source |
|   hardness error   | MAXI & Swift/BAT | Only available if MAXI and Swift/BAT data exists for this source |
|   combined flux   | MAXI & Swift/BAT | Only available if MAXI and Swift/BAT data exists for this source |
|   combined error   | MAXI & Swift/BAT | Only available if MAXI and Swift/BAT data exists for this source |

### Backend

The backend offers a standardized way to access data from the database.

### Frontend

The frontend

### Dashboards

For each source, a dashboard is provided, showing all available information. In this project, grafana (image version 10.4) is used to provide the dashboard environment

## Relevance Calculation

## Roadmap

[] Fix framework selection
[] Agree on licensing
[] Fully set up the container system

## Further Ideas

## Introduced Changes to FLASHES

### Necessary packages and software

- Docker (v28.0.1), incl. compose (v2.33.1)
- mongoDB (Docker image: mongo:6.0)
