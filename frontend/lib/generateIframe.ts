import type { SourceData } from '@/src/app/sources/[_id]/page';

export function generateURL({ data }: { data: SourceData }) {
    const has = {
        swift: data.swift != null,
        maxi:  data.maxi  != null,
        fermi: data.fermi != null,
    };

    var influxkey_chain = "";
    if (has.swift && !has.maxi && !has.fermi) {
        influxkey_chain = "&var-swift_influxkey="+encodeURIComponent(data.swift?.influx_key);
        return "http://localhost:3001/d-solo/flashes-swift/dashboard-for-swiftbat?var-integral_name="+encodeURIComponent(data.integral_name)+influxkey_chain+"&orgId=1&from=now-7d&to=now&timezone=browser&theme=dark&panelId=panel-1&kiosk=tv&__feature.dashboardSceneSolo=true";
    }
    else if (!has.swift && has.maxi && !has.fermi) {
        influxkey_chain = "&var-maxi_influxkey="+encodeURIComponent(data.maxi?.influx_key);
        return "http://localhost:3001/d-solo/flashes-maxi/dashboard-for-maxi?var-integral_name="+encodeURIComponent(data.integral_name)+influxkey_chain+"&orgId=1&from=now-7d&to=now&timezone=browser&theme=dark&panelId=panel-1&kiosk=tv&__feature.dashboardSceneSolo=true";
    }
    else if (has.swift && has.maxi && !has.fermi) {
        influxkey_chain = "&var-swift_influxkey="+encodeURIComponent(data.swift?.influx_key)+"&var-maxi_influxkey="+encodeURIComponent(data.maxi?.influx_key)+"&var-hardness_influxkey="+encodeURIComponent(data.hardness_ratio?.influx_key)+"&var-combined_influxkey="+encodeURIComponent(data.combined?.influx_key);
        return "http://localhost:3001/d-solo/flashes-swift-maxi/dashboard-for-swiftbat-maxi?var-integral_name="+encodeURIComponent(data.integral_name)+influxkey_chain+"&orgId=1&from=now-7d&to=now&timezone=browser&panelId=panel-1&kiosk=tv&__feature.dashboardSceneSolo=true";
    }
    else if (has.swift && !has.maxi && has.fermi) {
        influxkey_chain = "&var-swift_influxkey="+encodeURIComponent(data.swift?.influx_key)+"&var-fermi_influxkey="+encodeURIComponent(data.fermi?.influx_key);
        return "http://localhost:3001/d-solo/flashes-swift-fermi/dashboard-for-swiftbat-fermigbm?var-integral_name="+encodeURIComponent(data.integral_name)+influxkey_chain+"&orgId=1&from=now-7d&to=now&timezone=browser&panelId=panel-3&kiosk=tv&__feature.dashboardSceneSolo=true";
    }
    else if (has.swift && has.maxi && has.fermi) {
        influxkey_chain = "&var-swift_influxkey="+encodeURIComponent(data.swift?.influx_key)+"&var-maxi_influxkey="+encodeURIComponent(data.maxi?.influx_key)+"&var-fermi_influxkey="+encodeURIComponent(data.fermi?.influx_key)+"&var-hardness_influxkey="+encodeURIComponent(data.hardness_ratio?.influx_key)+"&var-combined_influxkey="+encodeURIComponent(data.combined?.influx_key);
        return "http://localhost:3000/d-solo/flashes-swift-maxi-fermi/dashboard-for-swiftbat-maxi-fermigbm?var-integral_name="+encodeURIComponent(data.integral_name)+influxkey_chain+"&orgId=1&from=now-7d&to=now&timezone=browser&panelId=panel-1&kiosk=tv&__feature.dashboardSceneSolo=true";
    }
    return null;

}