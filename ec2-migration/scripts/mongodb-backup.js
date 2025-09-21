#!/usr/bin/env node

/**
 * MongoDB Backup to S3
 * Backs up users and survey results collections to S3
 * Runs daily via cron job on EC2 instance
 */

const { MongoClient } = require('mongodb');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const fs = require('fs');
const path = require('path');
const { exec } = require('child_process');
const util = require('util');

// Configuration
const MONGODB_URI = process.env.MONGODB_URI;
const BACKUP_BUCKET = process.env.BACKUP_BUCKET;
const AWS_REGION = process.env.AWS_REGION || 'us-east-1';

// S3 Client
const s3Client = new S3Client({ region: AWS_REGION });

// Promisify exec
const execPromise = util.promisify(exec);

class MongoDBBackupService {
    constructor() {
        this.backupDir = '/tmp/mongodb-backups';
        this.logFile = '/opt/hahd/logs/backup.log';
    }

    log(message) {
        const timestamp = new Date().toISOString();
        const logMessage = `[${timestamp}] ${message}\n`;
        
        console.log(logMessage.trim());
        
        // Append to log file
        fs.appendFileSync(this.logFile, logMessage, { encoding: 'utf8' });
    }

    async ensureBackupDirectory() {
        try {
            if (!fs.existsSync(this.backupDir)) {
                fs.mkdirSync(this.backupDir, { recursive: true });
                this.log(`Created backup directory: ${this.backupDir}`);
            }
        } catch (error) {
            throw new Error(`Failed to create backup directory: ${error.message}`);
        }
    }

    async connectToMongoDB() {
        try {
            this.log('Connecting to MongoDB...');
            this.client = new MongoClient(MONGODB_URI);
            await this.client.connect();
            this.db = this.client.db('survey');
            this.log('Connected to MongoDB successfully');
        } catch (error) {
            throw new Error(`MongoDB connection failed: ${error.message}`);
        }
    }

    async backupCollection(collectionName) {
        try {
            this.log(`Starting backup of ${collectionName} collection...`);
            
            const collection = this.db.collection(collectionName);
            const count = await collection.countDocuments();
            
            if (count === 0) {
                this.log(`Collection ${collectionName} is empty, skipping backup`);
                return null;
            }

            this.log(`Found ${count} documents in ${collectionName}`);
            
            // Export to JSON
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const filename = `${collectionName}_backup_${timestamp}.json`;
            const filepath = path.join(this.backupDir, filename);
            
            // Get all documents
            const documents = await collection.find({}).toArray();
            
            // Write to file
            const backupData = {
                collection: collectionName,
                timestamp: new Date().toISOString(),
                count: documents.length,
                documents: documents
            };
            
            fs.writeFileSync(filepath, JSON.stringify(backupData, null, 2));
            
            this.log(`Backup written to: ${filepath}`);
            this.log(`File size: ${(fs.statSync(filepath).size / 1024 / 1024).toFixed(2)} MB`);
            
            return filepath;
        } catch (error) {
            throw new Error(`Failed to backup ${collectionName}: ${error.message}`);
        }
    }

    async uploadToS3(filepath, collectionName) {
        try {
            const filename = path.basename(filepath);
            const s3Key = `mongodb-backups/${collectionName}/${filename}`;
            
            this.log(`Uploading ${filename} to S3...`);
            
            const fileContent = fs.readFileSync(filepath);
            
            const uploadParams = {
                Bucket: BACKUP_BUCKET,
                Key: s3Key,
                Body: fileContent,
                ContentType: 'application/json',
                Metadata: {
                    'backup-date': new Date().toISOString(),
                    'collection': collectionName,
                    'source': 'mongodb-backup-service'
                }
            };
            
            const command = new PutObjectCommand(uploadParams);
            await s3Client.send(command);
            
            this.log(`Successfully uploaded to S3: s3://${BACKUP_BUCKET}/${s3Key}`);
            
            // Clean up local file
            fs.unlinkSync(filepath);
            this.log(`Cleaned up local file: ${filepath}`);
            
            return s3Key;
        } catch (error) {
            throw new Error(`S3 upload failed: ${error.message}`);
        }
    }

    async generateBackupReport(backupResults) {
        const report = {
            timestamp: new Date().toISOString(),
            success: backupResults.every(result => result.success),
            backups: backupResults,
            summary: {
                total_collections: backupResults.length,
                successful_backups: backupResults.filter(r => r.success).length,
                failed_backups: backupResults.filter(r => !r.success).length,
                total_documents: backupResults.reduce((sum, r) => sum + (r.document_count || 0), 0)
            }
        };

        const reportFilename = `backup_report_${new Date().toISOString().replace(/[:.]/g, '-')}.json`;
        const reportPath = path.join(this.backupDir, reportFilename);
        
        fs.writeFileSync(reportPath, JSON.stringify(report, null, 2));
        
        // Upload report to S3
        const reportS3Key = `mongodb-backups/reports/${reportFilename}`;
        const command = new PutObjectCommand({
            Bucket: BACKUP_BUCKET,
            Key: reportS3Key,
            Body: JSON.stringify(report, null, 2),
            ContentType: 'application/json'
        });
        
        await s3Client.send(command);
        fs.unlinkSync(reportPath);
        
        this.log(`Backup report uploaded: s3://${BACKUP_BUCKET}/${reportS3Key}`);
        
        return report;
    }

    async runBackup() {
        const startTime = Date.now();
        let backupResults = [];

        try {
            this.log('='.repeat(60));
            this.log('Starting MongoDB backup process');
            this.log('='.repeat(60));

            if (!MONGODB_URI || !BACKUP_BUCKET) {
                throw new Error('Missing required environment variables: MONGODB_URI, BACKUP_BUCKET');
            }

            await this.ensureBackupDirectory();
            await this.connectToMongoDB();

            // Collections to backup
            const collections = ['users', 'results'];
            
            for (const collectionName of collections) {
                try {
                    this.log(`\n--- Backing up ${collectionName} ---`);
                    
                    const filepath = await this.backupCollection(collectionName);
                    
                    if (filepath) {
                        const s3Key = await this.uploadToS3(filepath, collectionName);
                        
                        // Get collection stats
                        const collection = this.db.collection(collectionName);
                        const count = await collection.countDocuments();
                        
                        backupResults.push({
                            collection: collectionName,
                            success: true,
                            s3_key: s3Key,
                            document_count: count,
                            timestamp: new Date().toISOString()
                        });
                    } else {
                        backupResults.push({
                            collection: collectionName,
                            success: true,
                            message: 'Collection empty, skipped backup',
                            document_count: 0,
                            timestamp: new Date().toISOString()
                        });
                    }
                    
                    this.log(`✅ ${collectionName} backup completed successfully`);
                    
                } catch (error) {
                    this.log(`❌ Failed to backup ${collectionName}: ${error.message}`);
                    backupResults.push({
                        collection: collectionName,
                        success: false,
                        error: error.message,
                        timestamp: new Date().toISOString()
                    });
                }
            }

            // Generate and upload backup report
            const report = await this.generateBackupReport(backupResults);
            
            const endTime = Date.now();
            const duration = ((endTime - startTime) / 1000).toFixed(2);
            
            this.log('\n' + '='.repeat(60));
            this.log('Backup Summary:');
            this.log(`Duration: ${duration} seconds`);
            this.log(`Total Collections: ${report.summary.total_collections}`);
            this.log(`Successful Backups: ${report.summary.successful_backups}`);
            this.log(`Failed Backups: ${report.summary.failed_backups}`);
            this.log(`Total Documents Backed Up: ${report.summary.total_documents}`);
            this.log('='.repeat(60));
            
            return report;

        } catch (error) {
            this.log(`❌ Backup process failed: ${error.message}`);
            throw error;
        } finally {
            if (this.client) {
                await this.client.close();
                this.log('MongoDB connection closed');
            }
            
            // Clean up backup directory
            try {
                const files = fs.readdirSync(this.backupDir);
                for (const file of files) {
                    fs.unlinkSync(path.join(this.backupDir, file));
                }
                this.log('Cleaned up temporary backup files');
            } catch (error) {
                this.log(`Warning: Failed to clean up backup directory: ${error.message}`);
            }
        }
    }
}

// Health check function
async function healthCheck() {
    try {
        const client = new MongoClient(MONGODB_URI);
        await client.connect();
        const db = client.db('survey');
        
        const collections = await db.listCollections().toArray();
        const stats = {};
        
        for (const col of collections) {
            const collection = db.collection(col.name);
            stats[col.name] = await collection.countDocuments();
        }
        
        await client.close();
        
        console.log('MongoDB Health Check Results:');
        console.log(JSON.stringify(stats, null, 2));
        
        return stats;
    } catch (error) {
        console.error('Health check failed:', error.message);
        process.exit(1);
    }
}

// CLI Interface
async function main() {
    const command = process.argv[2];
    
    try {
        switch (command) {
            case 'backup':
                const service = new MongoDBBackupService();
                const report = await service.runBackup();
                
                if (report.success) {
                    console.log('✅ Backup completed successfully');
                    process.exit(0);
                } else {
                    console.log('❌ Backup completed with errors');
                    process.exit(1);
                }
                break;
                
            case 'health':
                await healthCheck();
                break;
                
            case 'test':
                // Test S3 connectivity
                try {
                    const testKey = `test/connectivity_test_${Date.now()}.txt`;
                    const command = new PutObjectCommand({
                        Bucket: BACKUP_BUCKET,
                        Key: testKey,
                        Body: 'S3 connectivity test',
                        ContentType: 'text/plain'
                    });
                    
                    await s3Client.send(command);
                    console.log(`✅ S3 connectivity test successful: s3://${BACKUP_BUCKET}/${testKey}`);
                } catch (error) {
                    console.error('❌ S3 connectivity test failed:', error.message);
                    process.exit(1);
                }
                break;
                
            default:
                console.log('Usage:');
                console.log('  node mongodb-backup.js backup  - Run full backup');
                console.log('  node mongodb-backup.js health  - Check MongoDB connectivity');
                console.log('  node mongodb-backup.js test    - Test S3 connectivity');
                break;
        }
    } catch (error) {
        console.error('Script failed:', error.message);
        process.exit(1);
    }
}

// Run if called directly
if (require.main === module) {
    main();
}

module.exports = { MongoDBBackupService };