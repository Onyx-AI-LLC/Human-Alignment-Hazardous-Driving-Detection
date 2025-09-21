import { Routes, Route, Navigate } from 'react-router-dom';
import { useAuth } from './hooks/useAuth';
import './App.css'
import './styles/hahd.css'
import SignInPage from './pages/SignIn/SignInPage';
import RegistrationPage from './pages/Registration/RegistrationPage';
import LandingPage from './pages/LandingPage/LandingPage';
import Calibration from './pages/Calibration/Calibration';
import Survey from './pages/Survey/Survey';
import Models from './pages/Models/Models';
import { useWebGazer } from './hooks/useWebGazer';

const App: React.FC = () => {
  const { user } = useAuth();
  const { isCalibrated } = useWebGazer();

  return (
    <Routes>
      {/* Legacy root route redirects to /hahd for backwards compatibility */}
      <Route path='/' element={<Navigate to='/hahd' replace />} />
      
      {/* HAHD Study Routes */}
      <Route path='/hahd' element={user ? <Navigate to='/hahd/dashboard' /> : <SignInPage />} />
      <Route path='/hahd/models' element={<Models />} />
      <Route path='/hahd/registration' element={<RegistrationPage />} />
      <Route 
        path='/hahd/dashboard' 
        element={!user ? <Navigate to='/hahd' /> : <LandingPage />} 
      />
      <Route 
        path='/hahd/calibration'
        element={
          !user ? <Navigate to='/hahd' /> :
          isCalibrated ? <Navigate to='/hahd/survey' /> : <Calibration />
        } 
      />
      <Route 
        path='/hahd/survey' 
        element={
          !user ? <Navigate to='/hahd' /> :
          !isCalibrated ? <Navigate to='/hahd/dashboard' /> : <Survey />
        } 
      />
      
      {/* Legacy route redirects for backwards compatibility */}
      <Route path='/models' element={<Navigate to='/hahd/models' replace />} />
      <Route path='/registration' element={<Navigate to='/hahd/registration' replace />} />
      <Route path='/landingpage' element={<Navigate to='/hahd/dashboard' replace />} />
      <Route path='/calibration' element={<Navigate to='/hahd/calibration' replace />} />
      <Route path='/survey' element={<Navigate to='/hahd/survey' replace />} />
    </Routes>
  )
}

export default App
