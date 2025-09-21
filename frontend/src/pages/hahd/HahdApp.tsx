import React from 'react';
import { Routes, Route, Navigate } from 'react-router-dom';
import { useAuth } from '../../hooks/useAuth';
import { useWebGazer } from '../../hooks/useWebGazer';
import SignInPage from '../SignIn/SignInPage';
import RegistrationPage from '../Registration/RegistrationPage';
import LandingPage from '../LandingPage/LandingPage';
import Calibration from '../Calibration/Calibration';
import Survey from '../Survey/Survey';
import Models from '../Models/Models';

const HahdApp: React.FC = () => {
  const { user } = useAuth();
  const { isCalibrated } = useWebGazer();

  return (
    <div className="hahd-app">
      <Routes>
        <Route path="/" element={user ? <Navigate to="/hahd/dashboard" /> : <SignInPage />} />
        <Route path="/registration" element={<RegistrationPage />} />
        <Route path="/models" element={<Models />} />
        <Route 
          path="/dashboard" 
          element={!user ? <Navigate to="/hahd" /> : <LandingPage />} 
        />
        <Route
          path="/calibration"
          element={
            !user ? <Navigate to="/hahd" /> :
            isCalibrated ? <Navigate to="/hahd/survey" /> : <Calibration />
          } 
        />
        <Route 
          path="/survey" 
          element={
            !user ? <Navigate to="/hahd" /> :
            !isCalibrated ? <Navigate to="/hahd/dashboard" /> : <Survey />
          } 
        />
      </Routes>
    </div>
  );
};

export default HahdApp;