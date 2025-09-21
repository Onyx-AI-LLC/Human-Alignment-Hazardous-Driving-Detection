import { API_BASE } from './config/api';

console.log('🔧 API Configuration Test:');
console.log('Environment:', import.meta.env.MODE);
console.log('API Base URL:', API_BASE);
console.log('All env vars:', import.meta.env);

// Test API connection
fetch(`${API_BASE}/health`)
  .then(response => response.json())
  .then(data => {
    console.log('✅ API Connection Test Successful:', data);
  })
  .catch(error => {
    console.error('❌ API Connection Test Failed:', error);
  });