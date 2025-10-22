export default function Home() {
  return (
    <main>
      <div className="space-y-4 text-neutral-300 leading-relaxed">
        <h1>Welcome to the FLASHES<span className="text-neutral-400">2.0</span>!</h1>
        <p className="space-y-4">
          This is the <b>FL</b>exible <b>A</b>lert <b>S</b>ystem for <b>H</b>igh <b>E</b>nergy <b>S</b>ources. FLASHES is a monitoring and analyses tool for x-ray sources in space. It provides lightcurve data from the <i>Monitor of All-sky X-ray image</i> (<a href="https://maxi.riken.jp/top/index.html" className="externalLink" target="_blank">MAXI</a>), the Burst Array Telescope (<a href="https://swift.gsfc.nasa.gov/about_swift/bat_desc.html" className="externalLink" target="_blank">BAT</a>) of Swift, and the Gamma-Ray Burst Monitor (<a href="https://fermi.gsfc.nasa.gov/science/instruments/gbm.html" className="externalLink" target="_blank">GBM</a>) on Fermi are automatically downloaded, processed and analysed every day. For each source in the FLASHES source catalog, a relevance is calculated. If a worth-mentioning event is happening, a high relevance value (close to 1) is assigned. If nothing happens, a low relevance value (around 0) is assigned.
        </p>
      
        <p className="space-y-4">
          The sources in the FLASHES catalogue are departed into several categories. The categorization is based on <a className="externalLink" target="_blank">HEASARC</a>. Each category has an overview table that can be assessed in the frontend. The tables provide an overview of the measurements for each source. The four most important tables are shown below. Additionally, each source has a detail page showing all available details and a dashboard for the timeseries.  
        </p>

        <p className="space-y-4">
          Use the navigation bars above to explore the complete source catalogue, select tags, and learn more about FLASHES2.0. Enjoy your stay!
        </p>
      </div>

      <div className="space-y-8 text-neutral-300 leading-relaxed">
      <h1 className="text-2xl font-semibold tracking-tight mt-10 mb-3">Want to contribute?</h1>
      <p className="space-y-4">
        FLASHES is still in development! If you want to contribute, feel free to explore the code in the repository on <a href="https://github.com/FinnWelzmueller/FLASHES2.0" className="externalLink" target="_blank">GitHub</a>. Any contributions are highly appreciated! If you found a bug or have a feature request, please open an issue <a href="https://github.com/FinnWelzmueller/FLASHES2.0/issues" className="externalLink" target="_blank">here</a>.
      </p>
      </div>
      <div className="mt-10 grid grid-cols-2 md:grid-cols-4 gap-3 text-center">
        <a 
        href='/tags/be-star' 
        className="rounded-xl border border-neutral-500 text-neutral-100 py-4 font-medium transition hover:text-neutral-900 hover:bg-gradient-to-br hover:from-neutral-400 hover:to-neutral-300">
                Be Star
        </a>
        <a 
        href='/tags/black-hole'
        className="rounded-xl border border-neutral-500 text-neutral-100 py-4 font-medium transition hover:text-neutral-900 hover:bg-gradient-to-br hover:from-neutral-400 hover:to-neutral-300">
                Black Hole
        </a>
        <a 
        href='/tags/burster'
        className="rounded-xl border border-neutral-500 text-neutral-100 py-4 font-medium transition hover:text-neutral-900 hover:bg-gradient-to-br hover:from-neutral-400 hover:to-neutral-300">
                Burster
        </a>
        <a 
        href='/tags/binary'
        className="rounded-xl border border-neutral-500 text-neutral-100 py-4 font-medium transition hover:text-neutral-900 hover:bg-gradient-to-br hover:from-neutral-400 hover:to-neutral-300">
                Binary
        </a>
      </div>
    </main>
  );
} 