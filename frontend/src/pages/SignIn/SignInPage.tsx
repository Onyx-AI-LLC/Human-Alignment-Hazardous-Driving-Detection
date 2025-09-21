import styles from './SignInPage.module.css';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Row from 'react-bootstrap/Row';
import { Modal, Container } from 'react-bootstrap';
import { SignInFormData } from '../../utils/types';
import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import useSignIn from '../../hooks/useSignIn';
import HahdHeader from '../../components/hahd/HahdHeader';

const TermsOfService = () => {
    const [isOpen, setIsOpen] = useState(false);

    return (
        <>
            <div className="text-center mt-4">
                <button
                    onClick={() => setIsOpen(true)}
                    style={{
                        color: '#fff',
                        background: 'none',
                        border: 'none',
                        textDecoration: 'underline',
                        cursor: 'pointer',
                        opacity: 0.8
                    }}
                    onMouseOver={(e) => e.currentTarget.style.opacity = '1'}
                    onMouseOut={(e) => e.currentTarget.style.opacity = '0.8'}
                >
                    Terms of Service
                </button>
            </div>

            <Modal show={isOpen} onHide={() => setIsOpen(false)} size="lg" centered>
                <Modal.Header closeButton className="bg-dark text-white">
                    <Modal.Title>Terms of Service - ONYX AI LLC Raffle Program</Modal.Title>
                </Modal.Header>
                <Modal.Body style={{ maxHeight: '70vh', overflow: 'auto' }}>
                    <div className="terms-content">
                        <section className="mb-4">
                            <h3 className="fw-bold border-bottom pb-2">1) How to Earn Raffle Entries</h3>
                            <div className="ps-3">
                                <p className="mb-2">• <strong>Registration Bonus:</strong> Receive 5 entries upon completing registration</p>
                                <p className="mb-2">• <strong>Referral Bonus:</strong> Earn 10 entries for each person you refer who completes 3 surveys</p>
                                <p className="mb-2">• <strong>Survey Participation:</strong> Receive 1 entry for each hazardous detection video watched and completed with meaningful survey responses</p>
                            </div>
                        </section>

                        <section className="mb-4">
                            <h3 className="fw-bold border-bottom pb-2">2) Eligibility Requirements</h3>
                            <div className="ps-3">
                                <p>Participation is open to legal residents of the following states where raffle participation is permitted:</p>
                                <div className="row ps-3">
                                    <div className="col-md-4">
                                        • California
                                        • Florida
                                        • New York
                                        • Texas
                                        • Illinois
                                    </div>
                                    <div className="col-md-4">
                                        • Pennsylvania
                                        • Ohio
                                        • Georgia
                                        • Michigan
                                        • New Jersey
                                    </div>
                                    <div className="col-md-4">
                                        • Virginia
                                        • Washington
                                        • Massachusetts
                                        • Arizona
                                        • Colorado
                                    </div>
                                </div>
                                <p className="mt-3">Participants must be:</p>
                                <p className="mb-2">• 18 years or older</p>
                                <p className="mb-2">• Not employed by or affiliated with ONYX AI LLC</p>
                                <p className="mb-2">• Not immediate family members of ONYX AI LLC employees</p>
                            </div>
                        </section>

                        <section className="mb-4">
                            <h3 className="fw-bold border-bottom pb-2">3) Winner Selection & Prize Distribution</h3>
                            <div className="ps-3">
                                <p className="mb-3"><strong>Selection Process:</strong></p>
                                <p className="mb-2">• Winners are selected randomly on the first day of each year</p>
                                <p className="mb-2">• Multiple entries increase chances of winning</p>
                                <p className="mb-2">• Winners will be notified via their registered email address</p>

                                <p className="mb-3 mt-4"><strong>Prize Details:</strong></p>
                                <p className="mb-2">• Prize Amount: $100 USD</p>
                                <p className="mb-2">• Distribution: ONYX AI LLC will process payment within 5 business days</p>
                                <p className="mb-2">• Payment Methods: PayPal, bank transfer, or digital gift card</p>
                                <p className="mb-2">• Winners must respond within 7 days of notification to claim prize</p>
                                <p className="mb-2">• Unclaimed prizes will not be rolled over to the next years drawing</p>
                            </div>
                        </section>

                        <section className="mb-4">
                            <h3 className="fw-bold border-bottom pb-2">4) Privacy & Data Protection</h3>
                            <div className="ps-3">
                                <p className="mb-2">• All personal information is encrypted and stored securely</p>
                                <p className="mb-2">• Email addresses are used only for raffle-related communications</p>
                                <p className="mb-2">• Survey responses are anonymized for research purposes</p>
                                <p className="mb-2">• ONYX AI LLC will never sell or share personal information with third parties</p>
                                <p className="mb-2">• Winners' names may be announced on our platform unless opted out</p>
                                <p className="mb-2">• Participants can request data deletion at any time</p>
                                <p className="mb-2">• View our full Privacy Policy for additional details</p>
                            </div>
                        </section>

                        <section className="mb-4">
                            <h3 className="fw-bold border-bottom pb-2">5) Modifications</h3>
                            <div className="ps-3">
                                <p className="mb-2">• ONYX AI LLC reserves the right to modify these terms at any time</p>
                                <p className="mb-2">• Changes will be announced via email with 30 days notice</p>
                                <p className="mb-2">• Continued participation after changes constitutes acceptance</p>
                            </div>
                        </section>
                    </div>
                </Modal.Body>
            </Modal>
        </>
    );
};

const SignInPage: React.FC = () => {
    const { signIn } = useSignIn();
    const [formData, setFormData] = useState<SignInFormData>({
        email: '',
        password: '',
    });
    const [error, setError] = useState('');
    const [isScreenTooSmall, setIsScreenTooSmall] = useState(false);

    useEffect(() => {
        const checkScreenSize = () => {
            setIsScreenTooSmall(window.innerWidth < 1062);
        };

        checkScreenSize();
        window.addEventListener('resize', checkScreenSize);
        return () => window.removeEventListener('resize', checkScreenSize);
    }, []);

    const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
        e.preventDefault();
        try {
            await signIn(formData);
        } catch (err) {
            setError('The password or email you entered was incorrect, please try again.')
        }
    };

    const handleChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement>) => {
        const { name, value } = e.target;
        setFormData((prevState) => ({
            ...prevState,
            [name]: value,
        }));
    };

    if (isScreenTooSmall) {
        return (
            <div className={styles.screenWarning}>
                <i className={`bi bi-emoji-frown-fill ${styles.floatingIcon}`}></i>
                <h2>Screen Size Too Small</h2>
                <p>
                    For the best experience with our<span style={{ fontWeight: '700', color: '#FFD700' }}> Human-Aligned Hazard Detection Survey</span>,
                    please increase your window size or switch to a larger device.
                    Your feedback is important to us!
                </p>
            </div>
        );
    }

    return (
        <>
            <HahdHeader />
            <Container fluid className="vh-100">
            <Row className="h-100">
                {/* First Column */}
                <Col
                    md={6}
                    className="d-flex flex-column justify-content-center align-items-center px-5">
                    <div className={styles.containerWrapper}>
                        <div className={styles.headerWrapper}>
                            <h1 className={`${styles.title} mb-4 text-center`}>
                                Welcome to the Human-Aligned Hazard Detection Survey.
                            </h1>
                            <div className={styles.contentWrapper}>
                                <p className={`${styles.content} mb-4`}>
                                    Human Aligned Hazardous Detection (HAHD) is a research initiative aimed at making driving behavior in autonomous systems more aligned with human decision-making.
                                </p>
                            </div>
                        </div>
                        <Form onSubmit={handleSubmit} style={{ width: '100%', maxWidth: '500px', color: '#4a5568' }}>
                            <Form.Group className={styles.formInput} controlId="formGridEmail">
                                <Form.Label>Email</Form.Label>
                                <Form.Control
                                    type="email"
                                    placeholder="name@example.com"
                                    name="email"
                                    value={formData.email}
                                    onChange={handleChange}
                                    required
                                />
                            </Form.Group>

                            <Form.Group className={styles.formInput} controlId="formGridPassword">
                                <Form.Label>Password</Form.Label>
                                <Form.Control
                                    type="password"
                                    placeholder="Enter Password"
                                    name="password"
                                    value={formData.password}
                                    onChange={handleChange}
                                    required
                                />
                            </Form.Group>

                            {error && (
                                <div className="m-4" style={{ color: 'red', fontSize: '15px', textAlign: 'left' }}>
                                    {error}
                                </div>
                            )}

                            <Button type="submit" className={styles.submitButton}>
                                Sign In
                            </Button>
                            <p className="mt-3 text-center fst-italic">
                                Don't have an account? <Link to="/hahd/registration">Sign up</Link>
                            </p>
                        </Form>
                    </div>
                </Col>

                {/* Second Column */}
                <Col md={6} className={styles.celebrationContainer}>
                    <div className={styles.celebrationSvg}>
                        <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 200 200">
                            {/* Popper Base */}
                            <path d="M60 140 L80 120 L90 130 L70 150Z" fill="#FFB02E" />

                            {/* Streamers */}
                            <g>
                                <path d="M90 120 Q120 80 150 70" stroke="#FF3B30" fill="none" strokeWidth="4" />
                                <path d="M85 115 Q100 70 130 50" stroke="#FF9500" fill="none" strokeWidth="4" />
                                <path d="M95 125 Q140 90 160 85" stroke="#FFCD00" fill="none" strokeWidth="4" />
                                <path d="M80 110 Q90 60 110 40" stroke="#34C759" fill="none" strokeWidth="4" />
                                <path d="M100 130 Q150 100 170 100" stroke="#AF52DE" fill="none" strokeWidth="4" />
                            </g>

                            {/* Confetti */}
                            <g>
                                <rect x="120" y="60" width="8" height="8" fill="#FF3B30" transform="rotate(30 124 64)" />
                                <rect x="140" y="90" width="8" height="8" fill="#FF9500" transform="rotate(45 144 94)" />
                                <rect x="100" y="50" width="8" height="8" fill="#FFCD00" transform="rotate(15 104 54)" />
                                <rect x="130" y="70" width="8" height="8" fill="#34C759" transform="rotate(60 134 74)" />
                                <rect x="150" y="80" width="8" height="8" fill="#AF52DE" transform="rotate(75 154 84)" />

                                <path d="M110 70 l2-6 2 6 6 2-6 2-2 6-2-6-6-2z" fill="#FF3B30" />
                                <path d="M145 65 l2-6 2 6 6 2-6 2-2 6-2-6-6-2z" fill="#FFCD00" />
                                <path d="M160 95 l2-6 2 6 6 2-6 2-2 6-2-6-6-2z" fill="#34C759" />
                            </g>

                            {/* Sparkles */}
                            <g fill="#FFD700">
                                <circle cx="115" cy="45" r="2" />
                                <circle cx="165" cy="85" r="2" />
                                <circle cx="135" cy="95" r="2" />
                                <circle cx="145" cy="45" r="2" />
                            </g>
                        </svg>
                    </div>
                    <div className={styles.celebrationText}>
                        <h2 className={styles.celebrationHeading}>After Registering</h2>
                        <p className={styles.celebrationSubtext}>
                            You'll Receive<br />
                            <span className={styles.highlightText}>5 Entries</span><br />
                            Towards a <span className={styles.prizeText}>$100</span> Raffle!
                        </p>
                        <p className={styles.additionalText} style={{ fontStyle: 'italic' }}>
                            <span className={styles.additionalHighlight}>+ 1 entry</span> for every survey completed
                        </p>
                    </div>
                    <TermsOfService />
                </Col>
            </Row>
        </Container>
        </>
    );
};

export default SignInPage;