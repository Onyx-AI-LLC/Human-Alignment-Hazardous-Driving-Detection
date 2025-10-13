/**
 * API Configuration
 * Centralized API endpoint configuration for different environments
 */

const API_CONFIG = {
  development: {
    baseURL: import.meta.env.VITE_API_URL || 'http://localhost:3001'
  },
  production: {
    baseURL: import.meta.env.VITE_API_URL || 'https://human-alignment-hazardous-driving.onrender.com'
  }
};

const ENV = import.meta.env.MODE || 'development';
export const API_BASE = API_CONFIG[ENV as keyof typeof API_CONFIG]?.baseURL || API_CONFIG.development.baseURL;

export default API_CONFIG;