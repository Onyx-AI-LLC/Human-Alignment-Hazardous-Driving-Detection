#!/usr/bin/env node

/**
 * Frontend API URL Update Script
 * Updates all hardcoded Render URLs to use the new EC2 server
 */

const fs = require('fs');
const path = require('path');

// Configuration
const OLD_URL = 'https://human-alignment-hazardous-driving.onrender.com';
const NEW_URL_PLACEHOLDER = '${process.env.REACT_APP_API_URL || \'http://localhost:3001\'}';

// Files to update with their expected API call patterns
const FILES_TO_UPDATE = [
    {
        path: 'src/hooks/useSignIn.ts',
        pattern: /https:\/\/human-alignment-hazardous-driving\.onrender\.com\/auth\/signIn/g,
        replacement: '${API_BASE}/auth/signIn'
    },
    {
        path: 'src/hooks/useRegister.ts', 
        pattern: /https:\/\/human-alignment-hazardous-driving\.onrender\.com\/auth\/register/g,
        replacement: '${API_BASE}/auth/register'
    },
    {
        path: 'src/hooks/useValidateReferral.ts',
        pattern: /https:\/\/human-alignment-hazardous-driving\.onrender\.com\/auth\/validateReferral/g,
        replacement: '${API_BASE}/auth/validateReferral'
    },
    {
        path: 'src/hooks/usePostResults.ts',
        pattern: /https:\/\/human-alignment-hazardous-driving\.onrender\.com\/survey\/results/g,
        replacement: '${API_BASE}/survey/results'
    },
    {
        path: 'src/components/Questions.tsx',
        pattern: /https:\/\/human-alignment-hazardous-driving\.onrender\.com\/survey\/top-raffle-entries/g,
        replacement: '${API_BASE}/survey/top-raffle-entries'
    },
    {
        path: 'src/components/VideoPlayer.tsx',
        pattern: /https:\/\/human-alignment-hazardous-driving\.onrender\.com\/api\/videos\/random/g,
        replacement: '${API_BASE}/api/videos/random'
    },
    {
        path: 'src/components/ModelVideoPlayer.tsx',
        pattern: /https:\/\/human-alignment-hazardous-driving\.onrender\.com\/api\/videos\/238/g,
        replacement: '${API_BASE}/api/videos/238'
    }
];

function findProjectRoot() {
    let currentDir = process.cwd();
    
    // Look for frontend directory
    const frontendPath = path.join(currentDir, 'frontend');
    if (fs.existsSync(frontendPath)) {
        return frontendPath;
    }
    
    // Look for package.json with React dependencies
    const packageJsonPath = path.join(currentDir, 'package.json');
    if (fs.existsSync(packageJsonPath)) {
        const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
        if (packageJson.dependencies && (packageJson.dependencies.react || packageJson.devDependencies.react)) {
            return currentDir;
        }
    }
    
    throw new Error('Frontend project root not found. Run this script from the project root or frontend directory.');
}

function createApiConfigFile(frontendRoot) {
    const configContent = `/**
 * API Configuration
 * Centralized API endpoint configuration for different environments
 */

const API_CONFIG = {
  development: {
    baseURL: 'http://localhost:3001'
  },
  production: {
    baseURL: process.env.REACT_APP_API_URL || 'http://localhost:3001'
  }
};

const ENV = process.env.NODE_ENV || 'development';
export const API_BASE = API_CONFIG[ENV]?.baseURL || API_CONFIG.development.baseURL;

export default API_CONFIG;
`;

    const configPath = path.join(frontendRoot, 'src', 'config', 'api.ts');
    const configDir = path.dirname(configPath);
    
    if (!fs.existsSync(configDir)) {
        fs.mkdirSync(configDir, { recursive: true });
    }
    
    fs.writeFileSync(configPath, configContent);
    console.log(`Created API configuration file: ${configPath}`);
    
    return configPath;
}

function updateFile(frontendRoot, fileConfig) {
    const filePath = path.join(frontendRoot, fileConfig.path);
    
    if (!fs.existsSync(filePath)) {
        console.log(`⚠️  File not found: ${filePath}`);
        return false;
    }
    
    let content = fs.readFileSync(filePath, 'utf8');
    const originalContent = content;
    
    // Add import for API_BASE if not present
    const apiImport = "import { API_BASE } from '../config/api';";
    const configImport = "import { API_BASE } from '../../config/api';";
    
    let importToAdd = apiImport;
    if (fileConfig.path.includes('components/')) {
        importToAdd = configImport;
    }
    
    if (!content.includes('API_BASE') && !content.includes(apiImport) && !content.includes(configImport)) {
        // Find the right place to add import
        const importLines = content.match(/^import.*$/gm) || [];
        if (importLines.length > 0) {
            const lastImportIndex = content.lastIndexOf(importLines[importLines.length - 1]);
            const insertIndex = lastImportIndex + importLines[importLines.length - 1].length;
            content = content.slice(0, insertIndex) + '\n' + importToAdd + content.slice(insertIndex);
        } else {
            content = importToAdd + '\n\n' + content;
        }
    }
    
    // Replace the URL pattern
    content = content.replace(fileConfig.pattern, fileConfig.replacement);
    
    if (content !== originalContent) {
        fs.writeFileSync(filePath, content);
        console.log(`Updated: ${fileConfig.path}`);
        return true;
    } else {
        console.log(`⚠️  No changes needed: ${fileConfig.path}`);
        return false;
    }
}

function createEnvironmentFiles(frontendRoot) {
    // Create .env.development
    const devEnvContent = `# Development Environment
REACT_APP_API_URL=http://localhost:3001
REACT_APP_ENV=development
`;
    
    const devEnvPath = path.join(frontendRoot, '.env.development');
    fs.writeFileSync(devEnvPath, devEnvContent);
    console.log(`Created: .env.development`);
    
    // Create .env.production template
    const prodEnvContent = `# Production Environment
# Update this URL to your EC2 server URL
REACT_APP_API_URL=http://your-ec2-ip:3001
REACT_APP_ENV=production
`;
    
    const prodEnvPath = path.join(frontendRoot, '.env.production');
    if (!fs.existsSync(prodEnvPath)) {
        fs.writeFileSync(prodEnvPath, prodEnvContent);
        console.log(`Created: .env.production (template)`);
    } else {
        console.log(`⚠️  .env.production already exists, not overwriting`);
    }
    
    // Update .gitignore
    const gitignorePath = path.join(frontendRoot, '.gitignore');
    if (fs.existsSync(gitignorePath)) {
        let gitignoreContent = fs.readFileSync(gitignorePath, 'utf8');
        if (!gitignoreContent.includes('.env.local')) {
            gitignoreContent += '\n# Environment variables\n.env.local\n.env.*.local\n';
            fs.writeFileSync(gitignorePath, gitignoreContent);
            console.log(`Updated .gitignore`);
        }
    }
}

function updatePackageJsonScripts(frontendRoot) {
    const packageJsonPath = path.join(frontendRoot, 'package.json');
    if (!fs.existsSync(packageJsonPath)) {
        console.log('⚠️  package.json not found');
        return;
    }
    
    const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, 'utf8'));
    
    // Add build scripts for different environments
    if (!packageJson.scripts) {
        packageJson.scripts = {};
    }
    
    packageJson.scripts['build:dev'] = 'REACT_APP_ENV=development npm run build';
    packageJson.scripts['build:prod'] = 'REACT_APP_ENV=production npm run build';
    
    fs.writeFileSync(packageJsonPath, JSON.stringify(packageJson, null, 2));
    console.log(`Updated package.json scripts`);
}

function createUpdateInstructions(frontendRoot, serverUrl = 'YOUR_EC2_SERVER_URL') {
    const instructions = `# Frontend API Configuration Update

## What was changed:

1. Created \`src/config/api.ts\` for centralized API configuration
2. Updated all API calls to use environment-based URLs
3. Created \`.env.development\` and \`.env.production\` files
4. Added build scripts for different environments

## Next steps:

### 1. Update your production environment URL

Edit \`.env.production\`:
\`\`\`
REACT_APP_API_URL=${serverUrl}
\`\`\`

### 2. Test locally

\`\`\`bash
npm start
# Should connect to http://localhost:3001 (your local server or EC2)
\`\`\`

### 3. Build for production

\`\`\`bash
npm run build:prod
# This will use the URL from .env.production
\`\`\`

### 4. Deploy to HostGator

Upload the contents of the \`build/\` or \`dist/\` directory to your HostGator hosting.

### 5. Test all functionality

- [ ] User registration
- [ ] User sign in  
- [ ] Video playback
- [ ] Survey submission
- [ ] Raffle leaderboard

## Rollback plan

If something doesn't work:

1. Revert the changes:
   \`\`\`bash
   git checkout HEAD -- src/
   \`\`\`

2. Or manually update \`.env.production\`:
   \`\`\`
   REACT_APP_API_URL=https://human-alignment-hazardous-driving.onrender.com
   \`\`\`

## Files modified:

${FILES_TO_UPDATE.map(f => `- ${f.path}`).join('\n')}

## Configuration files created:

- \`src/config/api.ts\` - API configuration
- \`.env.development\` - Development environment
- \`.env.production\` - Production environment (update the URL!)
`;

    const instructionsPath = path.join(frontendRoot, 'FRONTEND_UPDATE_INSTRUCTIONS.md');
    fs.writeFileSync(instructionsPath, instructions);
    console.log(`Created: FRONTEND_UPDATE_INSTRUCTIONS.md`);
}

function main() {
    try {
        console.log('Starting frontend API URL update...\n');
        
        const frontendRoot = findProjectRoot();
        console.log(`📁 Frontend root: ${frontendRoot}\n`);
        
        // Create API configuration
        createApiConfigFile(frontendRoot);
        console.log('');
        
        // Update files
        console.log('🔄 Updating API calls in files...');
        let updatedFiles = 0;
        
        for (const fileConfig of FILES_TO_UPDATE) {
            if (updateFile(frontendRoot, fileConfig)) {
                updatedFiles++;
            }
        }
        
        console.log(`\n📊 Updated ${updatedFiles}/${FILES_TO_UPDATE.length} files\n`);
        
        // Create environment files
        console.log('📝 Creating environment configuration...');
        createEnvironmentFiles(frontendRoot);
        console.log('');
        
        // Update package.json
        console.log('📦 Updating build scripts...');
        updatePackageJsonScripts(frontendRoot);
        console.log('');
        
        // Create instructions
        createUpdateInstructions(frontendRoot, process.argv[2]);
        
        console.log('Frontend update completed successfully!');
        console.log('\n📋 Next steps:');
        console.log('1. Update .env.production with your EC2 server URL');
        console.log('2. Read FRONTEND_UPDATE_INSTRUCTIONS.md for details');
        console.log('3. Test locally: npm start');
        console.log('4. Build for production: npm run build:prod');
        console.log('5. Deploy to HostGator');
        
    } catch (error) {
        console.error('Error updating frontend:', error.message);
        process.exit(1);
    }
}

// CLI usage
if (process.argv.length > 2 && process.argv[2] === '--help') {
    console.log('Usage: node update-api-urls.js [EC2_SERVER_URL]');
    console.log('');
    console.log('Example:');
    console.log('  node update-api-urls.js http://3.45.67.89:3001');
    console.log('');
    process.exit(0);
}

if (require.main === module) {
    main();
}

module.exports = { updateFile, createApiConfigFile };