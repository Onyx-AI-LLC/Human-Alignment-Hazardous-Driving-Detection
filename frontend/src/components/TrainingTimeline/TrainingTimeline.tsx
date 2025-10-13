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
    accuracy: 8,
    precision: 6,
    recall: 11,
    f1Score: 7,
    epoch: 1,
    video: demoVideo
  },
  {
    id: 2,
    simulations: 100,
    accuracy: 12,
    precision: 9,
    recall: 15,
    f1Score: 11,
    epoch: 2,
    video: demoVideo
  },
  {
    id: 3,
    simulations: 200,
    accuracy: 17,
    precision: 14,
    recall: 21,
    f1Score: 16,
    epoch: 3,
    video: demoVideo
  },
  {
    id: 4,
    simulations: 350,
    accuracy: 19,
    precision: 23,
    recall: 16,
    f1Score: 18,
    epoch: 4,
    video: demoVideo
  },
  {
    id: 5,
    simulations: 500,
    accuracy: 24,
    precision: 18,
    recall: 28,
    f1Score: 22,
    epoch: 5,
    video: demoVideo
  },
  {
    id: 6,
    simulations: 750,
    accuracy: 29,
    precision: 26,
    recall: 33,
    f1Score: 28,
    epoch: 6,
    video: demoVideo
  },
  {
    id: 7,
    simulations: 1000,
    accuracy: 42,
    precision: 35,
    recall: 38,
    f1Score: 27,
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
      // Only try to play if the video is ready
      const video = videoRef.current;
      const tryPlay = () => {
        video.play().catch(() => {
          // Silently handle autoplay rejection - browser may block autoplay
        });
      };
      
      if (video.readyState >= 2) {
        tryPlay();
      } else {
        video.addEventListener('canplay', tryPlay, { once: true });
      }
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