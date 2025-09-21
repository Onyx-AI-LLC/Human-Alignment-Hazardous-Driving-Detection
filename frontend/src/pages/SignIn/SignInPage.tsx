import styles from './SignInPage.module.css';
import Button from 'react-bootstrap/Button';
import Col from 'react-bootstrap/Col';
import Form from 'react-bootstrap/Form';
import Row from 'react-bootstrap/Row';
import { Container } from 'react-bootstrap';
import { SignInFormData } from '../../utils/types';
import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import useSignIn from '../../hooks/useSignIn';
import HahdHeader from '../../components/hahd/HahdHeader';
import TrainingTimeline from '../../components/TrainingTimeline/TrainingTimeline';


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
            <Container fluid style={{ height: 'calc(100vh - 80px)' }}>
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

                {/* Second Column - Training Timeline */}
                <Col md={6} className="d-flex align-items-center justify-content-center p-0" style={{ height: 'calc(100vh - 80px)' }}>
                    <TrainingTimeline />
                </Col>
            </Row>
        </Container>
        </>
    );
};

export default SignInPage;