/**
 * API Configuration
 * Centralized API endpoint configuration for different environments
 */

const API_CONFIG = {
  development: {
    baseURL: 'http://localhost:3001'
  },
  production: {
    baseURL: import.meta.env.VITE_API_URL || 'http://13.222.122.86'
  }
};

const ENV = import.meta.env.MODE || 'development';
export const API_BASE = API_CONFIG[ENV as keyof typeof API_CONFIG]?.baseURL || API_CONFIG.development.baseURL;

export default API_CONFIG;