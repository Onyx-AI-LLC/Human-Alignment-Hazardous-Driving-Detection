import React from 'react';

const HahdHeader: React.FC = () => {
  return (
    <header className="hahd-header">
      <nav className="navbar">
        <div className="nav-container">
          <div className="nav-brand">
            <a href="/" className="company-link">
              <img src="/RealOnyxLogo.jpg" alt="Onyx AI" className="logo-image" />
            </a>
            <div className="study-info">
              <span className="study-title">HAHD Study</span>
              <span className="study-subtitle">Human-Aligned Hazardous Driving Detection</span>
            </div>
          </div>
          
          <div className="nav-actions">
            <a href="/" className="btn btn-text">← Back to Onyx AI</a>
          </div>
        </div>
      </nav>
    </header>
  );
};

export default HahdHeader;