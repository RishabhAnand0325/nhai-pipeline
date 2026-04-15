import { v4 as uuidv4 } from 'uuid';
import { processVideoFile } from './videoProcessing';
import { processGpxFile } from './gpxProcessing';

export const prepareData = async (videoFile, gpxFile, roadName, userEmail, userName, videoLength, fileSize, startAddress, endAddress, roadType = "LHS", startLatitude = "", startLongitude = "", endLatitude = "", endLongitude = "" ) => {
    try {
        const uid = uuidv4();
        console.log('Processing video file...');
        const { renamedFile, fileSize, videoLength } = await processVideoFile(videoFile);
        console.log('Video file processed successfully.');
        const newFilePath = `${uid}/${renamedFile.name}`;
        console.log('Processing GPX file...');
        const { positionDetails, startAddress, endAddress } = await processGpxFile(gpxFile, videoLength);
        console.log('GPX file processed successfully.');
        const data = {
            uid,
            positionDetails,
            filePath: newFilePath,
            videoLength,
            fileSize,
            title: `${roadName}`,
            isUploaded: false,
            userEmail,
            userName,
            createdAt: formatDate(new Date()),
            startAddress,
            endAddress,
            roadType,
            startLatitude,
            startLongitude,
            endLatitude,
            endLongitude,
        };
        return { data, uid };
    } catch (error) {
        throw new Error(`Error preparing data: ${error.message}`);
    }
};

// Function to format the current date
const formatDate = (date) => {
    const options = {
        year: 'numeric', month: 'long', day: 'numeric',
        hour: '2-digit', minute: '2-digit', second: '2-digit',
    };
    return date.toLocaleDateString('en-US', options).replace(',', ' –');
};
