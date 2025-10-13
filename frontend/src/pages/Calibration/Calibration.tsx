import { useEffect, useState } from "react";
import { useWebGazer } from "../../hooks/useWebGazer";
import styles from './Calibration.module.css';

const Calibration: React.FC = () => {
  const { startWebGazer, setIsCalibrated, stopWebGazer } = useWebGazer();
  const coordinates = [
    { x: 100, y: 100 },
    { x: window.innerWidth / 2, y: 100 },
    { x: window.innerWidth - 100, y: 100 },
    { x: 100, y: window.innerHeight / 2 },
    { x: window.innerWidth / 2, y: window.innerHeight / 2 },
    { x: window.innerWidth - 100, y: window.innerHeight / 2 },
    { x: 100, y: window.innerHeight - 100 },
    { x: window.innerWidth / 2, y: window.innerHeight - 100 },
    { x: window.innerWidth - 100, y: window.innerHeight - 100 },
  ];

  const [calibrationState, setCalibrationState] = useState({
    x: 100,
    y: 100,
    coordinate: 0,
    clicks: 5,  // Start from 5 instead of 0
    calibrationComplete: false,
  });

  const setNextCoordinate = () => {
    setCalibrationState((prev) => {
      const nextClicks = prev.clicks - 1;  // Decrement instead of increment
      const isLastPoint = prev.coordinate === coordinates.length - 1;

      if (isLastPoint && nextClicks === 0) {  // Check for 0 instead of 5
        // Don't call setIsCalibrated here - do it in useEffect
        return { ...prev, clicks: nextClicks, calibrationComplete: true };
      }

      if (nextClicks === 0) {  // Check for 0 instead of 5
        const nextCoordinate = prev.coordinate + 1;
        return {
          x: coordinates[nextCoordinate]?.x,
          y: coordinates[nextCoordinate]?.y,
          coordinate: nextCoordinate,
          clicks: 5,  // Reset to 5 instead of 0
          calibrationComplete: false,
        };
      }

      return { ...prev, clicks: nextClicks };
    });
  };

  useEffect(() => {
    startWebGazer();
    return () => stopWebGazer();
  }, []);

  useEffect(() => {
    if (calibrationState.calibrationComplete) {
      setIsCalibrated(true);
    }
  }, [calibrationState.calibrationComplete, setIsCalibrated]);

  const handleDotClick = () => {
    setNextCoordinate();
  };

  // Calculate red intensity based on remaining clicks
  const redIntensity = 80 + ((5 - calibrationState.clicks) * (255 - 80) / 5);

  return (
    <div
      className={styles.calibrationPoint}
      style={{
        position: 'absolute',
        left: `${calibrationState.x}px`,
        top: `${calibrationState.y}px`,
        backgroundColor: `rgb(${redIntensity}, 0, 0)`,  // Use red with calculated intensity
        zIndex: 1000,
      }}
      onClick={handleDotClick}
    >
      {calibrationState.clicks}
    </div>
  );
};

export default Calibration;