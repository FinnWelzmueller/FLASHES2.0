import { BigButton } from "@/components/bigButton";
import "@/src/app/globals.css";
export default function Home() {
  return (
    <main>
      <div className="space-y-4 text-neutral-300 leading-relaxed ">
        <h1>Welcome to the FLASHES<span className="text-neutral-400">2.0</span>!</h1>
        <div className="grid grid-cols-[minmax(0,1fr)_auto] sm:grid-cols-1 gap-8 items-start">
        <div className="space-y-4 max-w-[70ch]">
        <p className="space-y-4">
          This is the <b>FL</b>exible <b>A</b>lert <b>S</b>ystem for <b>H</b>igh <b>E</b>nergy <b>S</b>ources. FLASHES is a monitoring and analyses tool for x-ray sources in space. It provides lightcurve data from the <i>Monitor of All-sky X-ray image</i> (<a href="https://maxi.riken.jp/top/index.html" className="externalLink" target="_blank">MAXI</a>), the <i>Burst Array Telescope</i> (<a href="https://swift.gsfc.nasa.gov/about_swift/bat_desc.html" className="externalLink" target="_blank">BAT</a>) of Swift, and the <i>Gamma-Ray Burst Monitor</i> (<a href="https://fermi.gsfc.nasa.gov/science/instruments/gbm.html" className="externalLink" target="_blank">GBM</a>) on Fermi are automatically downloaded, processed and analysed every day. For each source in the FLASHES source catalog, a relevance is calculated. If a worth-mentioning event is happening, a high relevance value (close to 1) is assigned. If nothing happens, a low relevance value (around 0) is assigned.
        </p>
      
        <p className="space-y-4">
          The sources in the FLASHES catalogue are departed into several categories. The categorization is based on <a className="externalLink" target="_blank">HEASARC</a>. Each category has an overview table that can be assessed in the frontend. The tables provide an overview of the measurements for each source. The four most important tables are shown below. Additionally, each source has a detail page showing all available details and a dashboard for the timeseries.  
        </p>

        <p className="space-y-4">
          Use the navigation bars above to explore the complete source catalogue, select tags, and learn more about FLASHES2.0. Enjoy your stay!
        </p>
        </div>
        <div className="justify-self-end sm:justify-self-center">
          <img
            src="http://localhost:8000/static/catalogue.png"
            alt="FLASHES source catalogue"
            className="w-full h-auto rounded-2xl object-contain border dark:border-neutral-800"
          />
        </div>
        </div>
      </div>

      <div className="space-y-8 text-neutral-300 leading-relaxed">
      <h1 className="text-2xl font-semibold tracking-tight mt-10 mb-3">The FLASHES2.0 Open <span className="text-red-400">&beta;</span></h1>
      <p>
        FLASHES2.0 is currently in open beta. This means that not all features are fully implemented and bugs may still occur. Currently, the following features are available:
      </p>
      <ul className="list-disc list-inside space-y-1">
        <li> Browse the whole FLASHES2.0 source catalog</li>
        <li> Filter sources based on automated tagging</li>
        <li> Find first details of every source (more details to come!)</li>
        <li> View daily updated flux data and (if available) hardness information on dedicated dashboards</li>
        <li> Download interesting datasets directly from the dashboards</li>
      </ul>
      <p className="space-y-1">
        More features are comming soon! Here is a short overview of planned features:
      </p>
      <ul className="list-disc list-inside space-y-1">
        <li> Relevance evaluation of each source based on their past to detect interesting features</li>
        <li> Alerting and advertizing of relevant sources in the tables, on the main page and via Email</li>
        <li> More detailed source information including relevances</li>
        <li> Common units for dashboards (MJD and mCrab)</li>
        <li> Build dashboards for an indivdual selection of sources without creating an account</li>
        <li> A map of the sky showing all sources for a better visual overview</li>
      </ul>
      <p className="space-y-4">

        Want to contribute? Feel free to explore the code in the repository on <a href="https://github.com/FinnWelzmueller/FLASHES2.0" className="externalLink" target="_blank">GitHub</a>. Any contributions are highly appreciated! If you found a bug or have a feature request, please open an issue <a href="https://github.com/FinnWelzmueller/FLASHES2.0/issues" className="externalLink" target="_blank">here</a>.
      </p>
      </div>
      <div className="mt-10 grid grid-cols-2 md:grid-cols-4 gap-3 text-center">
        <BigButton 
          href='/tags/be-star'
          name='BE STAR'
          key='be-star'/>
        
        <BigButton 
          href='/tags/black-hole'
          name='BLACK HOLE'
          key='black-hole'/>

        <BigButton 
        href='/tags/burster'
        name='BURSTER'
        key='burster'/>

        <BigButton 
        href='/tags/binary'
        name='BINARY'
        key='binary'/>
      </div>
    </main>
  );
} 