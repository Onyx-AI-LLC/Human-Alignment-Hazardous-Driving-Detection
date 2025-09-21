import React from 'react';

const HahdHeader: React.FC = () => {
  return (
    <>
      {/* Fixed Back Button - positioned like Profile component */}
      <div className="position-fixed top-0 start-0 p-3" style={{ zIndex: 9999 }}>
        <a href="/" className="btn btn-text">← Back to Onyx AI</a>
      </div>
      
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
          </div>
        </nav>
      </header>
    </>
  );
};

export default HahdHeader;