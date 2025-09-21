import React, { useState, useRef, useEffect } from 'react';
import styles from './Profile.module.css';
import useSignOut from "../../hooks/useSignOut";
import { useWebGazer } from "../../hooks/useWebGazer";
import { useNavigate } from 'react-router-dom';

const Profile: React.FC = () => {
    const { stopWebGazer } = useWebGazer();
    const { signOut } = useSignOut();
    const navigate = useNavigate();
    const [isOpen, setIsOpen] = useState<boolean>(false);
    const dropdownRef = useRef<HTMLDivElement>(null);
    const [username, setUsername] = useState<string>('')
    const [numberOfSurveysCompleted, setNumberOfSurveysCompleted] = useState<number>(0);
    const [referralCode, setReferralCode] = useState<string>('');

    useEffect(() => {
        const handleClickOutside = (event: MouseEvent) => {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
                setIsOpen(false);
            }
        };

        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    const [isCopied, setIsCopied] = useState(false);

    const shareReferralLink = () => {
        const baseUrl = window.location.origin;
        const referralLink = `${baseUrl}/registration?referral=${referralCode}`;
        
        if (navigator.share) {
            navigator.share({
                title: 'Join the Human-Aligned Hazardous Detection Survey',
                text: 'Use my referral code to register!',
                url: referralLink
            });
        } else {
            navigator.clipboard.writeText(referralLink);
            setIsCopied(true);
            setTimeout(() => setIsCopied(false), 2000);
        }
    };

    const handleLogout = () => {
        stopWebGazer();
        signOut();
    };

    const handleNavigateInstructions = () => {
        navigate('/hahd/dashboard');
    }

    useEffect(() => {
        const userItem = localStorage.getItem('user')

        if (userItem) {
            const user = JSON.parse(userItem);
            setUsername(user.email);
            setNumberOfSurveysCompleted(user.surveysCompleted);
            setReferralCode(user.referralCode);
        };
    })

    return (
        <div className="position-fixed top-0 end-0 p-3" ref={dropdownRef} style={{ zIndex: 9999 }}>
            <div className={styles.profileImage}>
                <i
                    className="bi bi-person-circle"
                    onClick={() => setIsOpen(!isOpen)}
                    style={{ fontSize: '34px', color: '#2d3748' }}
                ></i>
            </div>

            {isOpen && (
                <div className={styles.dropdownWrapper}>
                    <div className={styles.subDropdown}>
                        <div className={styles.userInfo}>
                            <i className="bi bi-person-circle" style={{ fontSize: '20px', paddingLeft: '12px' }}></i>
                            <div className={styles.dropdownItem} style={{ paddingLeft: '10px' }}>{username}</div>
                        </div>
                        <hr />
                        <div className={styles.dropdownItemWrapper}>
                            <div className={styles.dropdownRow}>
                                <div className="icon-wrapper">
                                    <div className='' style={{ fontSize: '14px', color: '#000000' }}>Surveys Completed:</div>
                                </div>
                                <div className={styles.dropdownItem}>{numberOfSurveysCompleted}</div>
                            </div>
                        </div>
                        <div className={styles.dropdownItemWrapper} onClick={shareReferralLink}>
                            <div className={styles.dropdownRow}>
                                <div className="icon-wrapper">
                                    <div className='' style={{ fontSize: '14px', color: '#000000' }}>Referral Code</div>
                                </div>
                                <div
                                    className="flex items-center gap-2"
                                >
                                    {isCopied ? (
                                        <div className="flex items-center gap-1">
                                            <i className="bi bi-check-lg text-green-600 transition-all duration-200 ease-in-out"
                                                style={{ fontSize: '16px', color: '#16a34a' }} />
                                            <span className="text-xs text-green-600 animate-fade-in" style={{ fontSize: '14px', color: '#16a34a' }}>Copied!</span>
                                        </div>
                                    ) : (
                                        <i className="bi bi-copy hover:text-gray-600 transition-all duration-200"
                                            style={{ fontSize: '14px', color: '#000000' }} />
                                    )}
                                </div>
                            </div>
                        </div>
                        <div className={styles.dropdownItemWrapper}>
                            <div className={styles.dropdownRow} onClick={handleNavigateInstructions}>
                                <div className="icon-wrapper">
                                    <div className={styles.dropdownItem}>Survey Instructions</div>
                                </div>
                                <i className="bi bi-book-half" style={{ fontSize: '18px', color: '#000000' }}></i>
                            </div>
                        </div>
                        <div className={styles.dropdownItemWrapper}>
                            <div className={styles.dropdownRow} onClick={handleLogout}>
                                <div className="icon-wrapper">
                                    <div className={styles.dropdownItem}>Log Out</div>
                                </div>
                                <i className="bi bi-box-arrow-right" style={{ fontSize: '18px', color: '#000000' }}></i>
                            </div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
};

export default Profile;