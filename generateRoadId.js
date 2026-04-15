const generateRoadId = (startLat, startLng, endLat, endLng) => {
    if (!startLat || !startLng || !endLat || !endLng) {
      return null;
    }

    const normalizedStartLat = String(startLat).trim();
    const normalizedStartLng = String(startLng).trim();
    const normalizedEndLat = String(endLat).trim();
    const normalizedEndLng = String(endLng).trim();

    const fingerprint = ${normalizedStartLat}-${normalizedStartLng}-${normalizedEndLat}-${normalizedEndLng};

    let hash = 0;
    for (let i = 0; i < fingerprint.length; i++) {
      const char = fingerprint.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash;
    }

    const uniqueNum = Math.abs(hash) % 1000000;
    return R${uniqueNum.toString().padStart(6, '0')};
  };