import { XMLParser } from 'fast-xml-parser';
import Geocode from 'react-geocode';
import { getDistance } from 'geolib';

Geocode.setApiKey(process.env.REACT_APP_GOOGLE_KEYS);

const parser = new XMLParser({
    ignoreAttributes: false,
    attributeNamePrefix: "",
});

export const processGpxFile = async (gpxFile, videoLength) => {
    try {
        const gpxContent = await gpxFile.text();
        const gpxData = parser.parse(gpxContent);

        // Validate GPX structure
        if (
            !gpxData.gpx ||
            !gpxData.gpx.trk ||
            !gpxData.gpx.trk.trkseg ||
            !gpxData.gpx.trk.trkseg.trkpt ||
            !Array.isArray(gpxData.gpx.trk.trkseg.trkpt) ||
            gpxData.gpx.trk.trkseg.trkpt.length === 0
        ) {
            throw new Error("No valid trackpoints found in GPX file.");
        }

        const positionDetails = await extractTrackpoints(gpxData);

        const start = positionDetails[0]?.[0];
        const end = positionDetails[positionDetails.length - 1]?.[0];

        if (!start || !end) {
            throw new Error("Insufficient trackpoint data to compute addresses.");
        }

        const startAddress = await reverseGeocode(start.latitude, start.longitude);
        const endAddress = await reverseGeocode(end.latitude, end.longitude);

        return { positionDetails, startAddress, endAddress };
    } catch (error) {
        throw new Error(`Error processing GPX file: ${error.message}`);
    }
};

const extractTrackpoints = async (gpxData) => {
    const trackpoints = [];
    const trkpts = gpxData.gpx.trk.trkseg.trkpt;

    for (let i = 0; i < trkpts.length; i++) {
        const point = trkpts[i];
        const latitude = parseFloat(point.lat);
        const longitude = parseFloat(point.lon);
        const timeElapsed = (i + 1) * 1.0;
        const currentTime = new Date(point.time).getTime();

        let speed = 0;
        let speedAccuracy = 0;

        if (i > 0) {
            const prevPoint = trkpts[i - 1];
            const prevLatitude = parseFloat(prevPoint.lat);
            const prevLongitude = parseFloat(prevPoint.lon);
            const prevTime = new Date(prevPoint.time).getTime();
            const distance = getDistance(
                { latitude, longitude },
                { latitude: prevLatitude, longitude: prevLongitude }
            );
            const timeDifference = (currentTime - prevTime) / 1000;

            if (timeDifference > 0) {
                speed = distance / timeDifference;
                speedAccuracy = distance / timeDifference;
            }
        }

        trackpoints.push([{
            timeElapsed,
            latitude,
            longitude,
            speed,
            speedAccuracy,
            orientation: 'portraitUp',
        }]);
    }

    return trackpoints;
};

const reverseGeocode = async (latitude, longitude) => {
    try {
        const response = await Geocode.fromLatLng(latitude.toString(), longitude.toString());

        if (response.status === "ZERO_RESULTS" || !response.results || response.results.length === 0) {
            console.warn(`No address found for coordinates (${latitude}, ${longitude})`);
            return "Unknown location";
        }

        return response.results[0].formatted_address;
    } catch (error) {
        console.error("Reverse geocoding error:", error);
        return "Unknown location";
    }
};
