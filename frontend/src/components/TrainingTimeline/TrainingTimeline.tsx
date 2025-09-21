import React, { useState, useEffect, useRef } from 'react';
import styles from './TrainingTimeline.module.css';
import demoVideo from '../../assets/hazard_detection_demo.mp4';

interface TrainingIteration {
  id: number;
  simulations: number;
  accuracy: number;
  precision: number;
  recall: number;
  f1Score: number;
  epoch: number;
  video: string;
}

// Mock training data - represents progressive AI training iterations
const trainingData: TrainingIteration[] = [
  {
    id: 1,
    simulations: 50,
    accuracy: 20,
    precision: 18,
    recall: 22,
    f1Score: 20,
    epoch: 1,
    video: demoVideo
  },
  {
    id: 2,
    simulations: 100,
    accuracy: 35,
    precision: 32,
    recall: 38,
    f1Score: 35,
    epoch: 2,
    video: demoVideo
  },
  {
    id: 3,
    simulations: 200,
    accuracy: 52,
    precision: 48,
    recall: 56,
    f1Score: 52,
    epoch: 3,
    video: demoVideo
  },
  {
    id: 4,
    simulations: 350,
    accuracy: 68,
    precision: 65,
    recall: 71,
    f1Score: 68,
    epoch: 4,
    video: demoVideo
  },
  {
    id: 5,
    simulations: 500,
    accuracy: 78,
    precision: 75,
    recall: 81,
    f1Score: 78,
    epoch: 5,
    video: demoVideo
  },
  {
    id: 6,
    simulations: 750,
    accuracy: 84,
    precision: 82,
    recall: 86,
    f1Score: 84,
    epoch: 6,
    video: demoVideo
  },
  {
    id: 7,
    simulations: 1000,
    accuracy: 89,
    precision: 87,
    recall: 91,
    f1Score: 89,
    epoch: 7,
    video: demoVideo
  }
];

const TrainingTimeline: React.FC = () => {
  const [currentIndex, setCurrentIndex] = useState(0);
  const videoRef = useRef<HTMLVideoElement>(null);
  const [isTransitioning, setIsTransitioning] = useState(false);

  // Auto-play video when current iteration changes
  useEffect(() => {
    if (videoRef.current) {
      videoRef.current.load();
      videoRef.current.play().catch(console.error);
    }
  }, [currentIndex]);

  const nextIteration = () => {
    if (isTransitioning || currentIndex >= trainingData.length - 1) return;
    
    setIsTransitioning(true);
    setCurrentIndex((prev) => prev + 1);
    
    setTimeout(() => setIsTransitioning(false), 500);
  };

  const prevIteration = () => {
    if (isTransitioning || currentIndex <= 0) return;
    
    setIsTransitioning(true);
    setCurrentIndex((prev) => prev - 1);
    
    setTimeout(() => setIsTransitioning(false), 500);
  };

  const currentIteration = trainingData[currentIndex];
  const nextIterationData = currentIndex < trainingData.length - 1 ? trainingData[currentIndex + 1] : null;
  const prevIterationData = currentIndex > 0 ? trainingData[currentIndex - 1] : null;

  return (
    <div className={styles.timelineContainer}>
      <div className={styles.timelineHeader}>
        <h2 className={styles.title}>AI Training Progress</h2>
        <p className={styles.subtitle}>Real-time model evolution through human feedback</p>
      </div>

      <div className={styles.rotisserie}>
        {/* Previous iteration (partially visible) */}
        {prevIterationData && (
          <div className={`${styles.iterationCard} ${styles.prevCard}`}>
            <div className={styles.iterationNumber}>#{prevIterationData.epoch}</div>
            <div className={styles.metrics}>
              <div className={styles.metricSmall}>{prevIterationData.simulations} Sims</div>
              <div className={styles.metricSmall}>{prevIterationData.accuracy}% Acc</div>
            </div>
          </div>
        )}

        {/* Current iteration (main focus) */}
        <div className={`${styles.iterationCard} ${styles.currentCard} ${isTransitioning ? styles.transitioning : ''}`}>
          <div className={styles.videoContainer}>
            <video
              ref={videoRef}
              className={styles.demoVideo}
              muted
              loop
              controls={false}
            >
              <source src={currentIteration.video} type="video/mp4" />
              Your browser does not support the video tag.
            </video>
            <div className={styles.videoOverlay}>
              <div className={styles.iterationBadge}>
                Training Epoch #{currentIteration.epoch}
              </div>
            </div>
          </div>
          
          <div className={styles.metricsContainer}>
            <div className={styles.simulationCount}>
              {currentIteration.simulations.toLocaleString()} User Simulations
            </div>
            
            <div className={styles.metricsGrid}>
              <div className={styles.metric}>
                <span className={styles.metricLabel}>Accuracy</span>
                <span className={styles.metricValue}>{currentIteration.accuracy}%</span>
              </div>
              <div className={styles.metric}>
                <span className={styles.metricLabel}>Precision</span>
                <span className={styles.metricValue}>{currentIteration.precision}%</span>
              </div>
              <div className={styles.metric}>
                <span className={styles.metricLabel}>Recall</span>
                <span className={styles.metricValue}>{currentIteration.recall}%</span>
              </div>
              <div className={styles.metric}>
                <span className={styles.metricLabel}>F1 Score</span>
                <span className={styles.metricValue}>{currentIteration.f1Score}%</span>
              </div>
            </div>
          </div>
        </div>

        {/* Next iteration (partially visible) */}
        {nextIterationData && (
          <div className={`${styles.iterationCard} ${styles.nextCard}`}>
            <div className={styles.iterationNumber}>#{nextIterationData.epoch}</div>
            <div className={styles.metrics}>
              <div className={styles.metricSmall}>{nextIterationData.simulations} Sims</div>
              <div className={styles.metricSmall}>{nextIterationData.accuracy}% Acc</div>
            </div>
          </div>
        )}
      </div>

      {/* Navigation Controls */}
      <div className={styles.controls}>
        <button 
          className={styles.controlButton}
          onClick={prevIteration}
          disabled={isTransitioning || currentIndex <= 0}
        >
          ← Previous
        </button>
        
        <div className={styles.indicators}>
          {trainingData.map((_, index) => (
            <div
              key={index}
              className={`${styles.indicator} ${index === currentIndex ? styles.active : ''}`}
              onClick={() => !isTransitioning && setCurrentIndex(index)}
            />
          ))}
        </div>
        
        <button 
          className={styles.controlButton}
          onClick={nextIteration}
          disabled={isTransitioning || currentIndex >= trainingData.length - 1}
        >
          Next →
        </button>
      </div>

      <div className={styles.progressInfo}>
        <p className={styles.progressText}>
          Showing iteration {currentIndex + 1} of {trainingData.length}
        </p>
      </div>
    </div>
  );
};

export default TrainingTimeline;