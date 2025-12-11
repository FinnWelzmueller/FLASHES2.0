import "@/src/app/globals.css";

export default async function About() {
  return (
    <main>
      <h1>About FLASHES<span className="text-neutral-400">2.0</span></h1>
      <h2 className="space-y-4 text-neutral-300 leading-relaxed ">What is FLASHES?</h2>
      <p>
        FLASHES (<b>FL</b>exible <b>A</b>lert <b>S</b>ystem for <b>H</b>igh-<b>E</b>nergy <b>S</b>ources) is a modern monitoring and analysis platform for variable X-ray sources across the sky. It is the successor to the original FLASHES system, which is currently operated by ESA and which you can access <a href="https://integral.esac.esa.int/flashes" className="externalLink">here</a>. The new version builds on the same idea—continuous all-sky activity monitoring—but extends it with a more robust backend, modern dashboards, improved data handling, and an architecture that allows full automation and easy deployment.
      </p>
      <p>
        The system continuously collects and analyses light-curve data from <a href="https://maxi.riken.jp/top/index.html" className="externalLink" target="_blank">MAXI</a>, <a href="https://swift.gsfc.nasa.gov/about_swift/bat_desc.html" className="externalLink" target="_blank">Swift/BAT</a>, and <a href="https://fermi.gsfc.nasa.gov/science/instruments/gbm.html" className="externalLink" target="_blank">Fermi/GBM</a>, covering several energy bands from 2 to 50 keV. With more than <b>700 sources</b> in the catalogue, FLASHES assesses every newly available datapoint and evaluates whether a source is behaving normally or showing signs of interesting evolution. Its long-term goal is not only to provide fast updates but to detect <i>unusual behaviour</i> early, ideally even before major outbursts or state changes become obvious in the light curves.
      </p>
      <p>
        In short, FLASHES aims to be an automated, easy-to-use, daily-updated surveillance system for high-energy astrophysics.
      </p>

      <h2>Why is FLASHES scientifically valuable?</h2>
      <p>
        FLASHES offers an integrated, data-driven way to understand rapidly changing X-ray sources. It unifies data from multiple telescopes, automatically updates all observations, highlights the latest behaviour, and provides intuitive tools for exploring variability across different energy ranges. Because the entire system is fully automated and containerised, it allows anyone to run,study and improve a complete multi-telescope monitoring pipeline without needing to manually handle data retrieval, processing, or plotting.
      </p>

      <h2>What can FLASHES do right now (in open beta)?</h2>
      <p>
        FLASHES is already fully capable of running an end-to-end monitoring pipeline.
      </p>
      <p>
        At the beta stage, the system automatically downloads new MAXI, Swift/BAT, and Fermi/GBM light curves every day. Rather than cleaning or reprocessing old data, FLASHES focuses on efficient incremental updates: only new points are imported, but the system still checks for provider-side corrections—especially relevant for Swift—and replaces earlier datapoints as needed to ensure consistency. Whenever Swift and MAXI both provide data, FLASHES computes hardness ratios and combined flux values through well-defined analytic expressions and their errors via Gaussian error propagation.
      </p>
      <p>
        Metadata and catalogue information are stored in a MongoDB, while all timeseries are stored in InfluxDB for fast access. Each source is tagged automatically based on classifications derived from HEASARC's taxonomy, making it easy to browse astrophysical categories without manual tagging. The user interface, built on Next.js and Tailwind CSS, offers a clean overview of all sources, their categories, and the most recent activity. Each source's detail page highlights the behaviour of the last week and provides direct access to an interactive Grafana dashboard.
      </p>
      <p>
        The dashboards themselves serve as the central tool for time-series exploration. Users can adjust the time range freely, toggle energy bands, and inspect fluxes and uncertainties interactively. One of the most practical features is that the exact data shown in any panel can be downloaded directly via the built-in panel links, making data export intuitive and reproducible.
      </p>
      <p>
        Finally, the entire framework is containerised. FLASHES can be deployed locally with minimal effort using Docker and Docker Compose, enabling an easy setup that mirrors the full production environment.
      </p>

      <h2>What will FLASHES do in the full release?</h2>
      <p>
        The major upcoming addition is a complete relevance model that evaluates the behaviour of each source in a data-driven manner. This includes recognising outbursts, deviations from typical patterns, spectral changes, or unusual absence of data. The system will also support a lightweight alerting mechanism, including temporary dynamic tags on sources and options for user-side alert subscriptions.
      </p>
      <p> 
      Additional improvements will focus on usability, expanded dashboards, and refined data products.
      </p>

      <h2>How to use FLASHES?</h2>
      <p>
        Using FLASHES is straightforward. The navigation bar leads directly to all main features. Selecting <b>Sources</b> opens the full catalogue, where entries can be sorted, searched, and filtered by astrophysical category by clicking on a Tag. A list of tags routing to the filtered sources are available by selecting <b>Tags</b>. Choosing any source opens its detail page, which provides coordinates, cross-links to SIMBAD, NED, and HEASARC, and a concise summary of the latest observations. For visual inspection, each page links to an interactive dashboard tailored to the available telescopes for that source.
      </p>
      <p>
        Inside a dashboard, users can explore long-term light curves, hardness-intensity diagrams, and combined fluxes. Time ranges can be adjusted, channels can be toggled, and flux uncertainties are displayed as shaded regions for clarity. Whenever particular data are shown on the screen—whether a zoomed-in segment, a specific energy band, or a combined view — the panel links allow downloading exactly that selection without using API endpoints. This makes data extraction both intuitive and consistent with what is visible on the dashboard.
      </p>

      <h2> The FLASHES Tech Stack</h2>
      <p>
        The system uses a modern, modular set of technologies. The backend is built on FastAPI and Python, using libraries such as NumPy, Pandas, Astropy, Astroquery, and APScheduler. MongoDB 8.0 stores source metadata, while InfluxDB manages all timeseries data. The frontend is implemented with Next.js, React, TypeScript, and Tailwind CSS. For a full list of libraries and versions, please refer to the <a href="https://github.com/FinnWelzmueller/FLASHES2.0" className="externalLink">GitHub Repository</a>.
      </p>
      <p>
        Grafana, together with the Infinity Datasource plugin, provides the dashboard environment, with automatically generated templates for every possible combination of available telescope data. Deployment is entirely container-based through Docker and Docker Compose.
      </p>

      <h2>Where does the data come from?</h2>
      <p>
        FLASHES retrieves publicly available light-curve data from three major X-ray missions. <a href="https://maxi.riken.jp/top/index.html" className="externalLink" target="_blank">MAXI</a> provides broadband 2-20 keV data along with several sub-bands. <a href="https://swift.gsfc.nasa.gov/about_swift/bat_desc.html" className="externalLink" target="_blank">Swift/BAT</a> delivers 15-50 keV monitoring data. <a href="https://fermi.gsfc.nasa.gov/science/instruments/gbm.html" className="externalLink" target="_blank">Fermi/GBM</a> contributes 12-50 keV fluxes for relevant sources.
      </p>
      <p>
        MAXI is mounted on the International Space Station and scans nearly the entire sky every 92 minutes. It offers broad spectral coverage from 2-20 keV together with finer sub-bands (2-4, 4-10, 10-20 keV), enabling the study of both soft and moderately hard X-ray emission. The instrument is particularly well-suited for identifying long-term brightness trends, soft outbursts, and thermal components in accreting systems. Its regular scanning cadence and stable long-baseline coverage make MAXI the backbone for tracking persistent sources as well as monitoring variability across hundreds of objects in the FLASHES catalogue. 
      </p>
      <p>
        Swift's Burst Alert Telescope provides continuous hard X-ray monitoring in the 15-50 keV range and is known for its sensitivity to sudden increases in flux, such as those from X-ray binaries, magnetars, and gamma-ray sources. BAT periodically revises its publicly available light-curve files as new calibrations or data corrections become available; FLASHES accounts for this by overwriting older datapoints when updated values appear. This makes Swift/BAT a reliable source for detecting early signatures of hard-X-ray activity, spectral hardening, or the onset of outbursts, complementing MAXI's softer energy coverage.
      </p>
      <p>
        Fermi/GBM contributes high-energy monitoring through its 12-50 keV Earth-occultation data products. While GBM primarily targets gamma-ray bursts, its continuous all-sky sensitivity provides valuable measurements of persistent and transient hard X-ray sources. GBM does not supply uncertainties for these flux values, so FLASHES stores error columns as zero to reflect the format of the original data. Despite this limitation, GBM plays an important role in extending the hard-X-ray coverage of FLASHES, especially for sources with strong non-thermal components or those exhibiting hard-state variability.
      </p>
      <p>
        The lightcurve data is downloaded in SI units (counts/cm²/s) and converted to mCrab fluxes using established conversion factors (see Cifuentes Santos, 2021) for each instrument. This standardisation allows for direct comparison across different telescopes and energy bands within the FLASHES platform.
      </p>
      <h2>Getting involved</h2>
      <p>
        FLASHES welcomes contributions from both the astrophysics and software communities.
      </p>
      <p>
        You can help by:
      </p>
      <ul className="list-disc list-inside space-y-1">
        <li> Reporting bugs </li>
        <li> Suggesting new features </li>
        <li> Proposing relevance algorithms </li>
        <li> Developing dashboard templates </li>
        <li> Enhancing documentation </li>
        <li> Adding support for additional telescopes </li>
        <li> Improving UI elements and user experience </li>
        <li> Testing the deployment process and providing feedback </li>
      </ul>
      <p>
        Contributions of any kind are appreciated and help shape the future capabilities of FLASHES.
      </p>

      <h2>There is one last thing</h2>
      <p>
        When you use FLASHES data for your publications, please add a reference to this website or mention us in the Acknowledgements. This helps us a lot. Thank you!
      </p>
    </main>
  );
}