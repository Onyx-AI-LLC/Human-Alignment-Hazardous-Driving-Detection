import { Button, Container } from "react-bootstrap";
import { useNavigate } from "react-router-dom";
import { Stepper, Step, StepLabel, Box, StepIcon } from "@mui/material";
import calibrationTutorial from "../../assets/CalibrationTutorialHAHD.mp4";
import simulationVideo from "../../assets/SimulationTutorial.mp4";
import questionnaireTutorial from "../../assets/QuestionnaireTutorial.mp4";
import { useState } from "react";
import Profile from "../../components/Profile/Profile";
import styles from "./LandingPage.module.css";
import HahdHeader from "../../components/hahd/HahdHeader";

const LandingPage: React.FC = () => {
    const navigate = useNavigate();
    const [activeStep, setActiveStep] = useState<number>(0);

    const steps = [
        "Eye-Tracker Calibration",
        "Observe Driving Footage",
        "Post-Simulation Survey",
        "End Instructions",
    ];

    const handleBeginCalibration = () => {
        const permissions = window.confirm(
            "The Human-Alignment Hazard Detection Survey would like to access your camera."
        );
        if (permissions) {
            navigate("/hahd/calibration");
        }
    };

    const handleNext = () => {
        setActiveStep((prevStep) => Math.min(prevStep + 1, steps.length - 1));
    };

    const handleBack = () => {
        setActiveStep((prevStep) => Math.max(prevStep - 1, 0));
    };

    const renderStepContent = (step: number) => {
        switch (step) {
            case 0:
                return (
                    <div className={styles.content}>
                        <p className={styles.description}>
                            Before starting, you'll need to calibrate the eye tracker. Please
                            ensure you are not wearing sunglasses.{" "}
                            <b>Grant camera access, sit up straight, and ensure proper lighting.</b>{" "}
                            You'll click on nine dots multiple times on the screen before
                            beginning the survey.
                        </p>
                        <video
                            src={calibrationTutorial}
                            className={styles.video}
                            autoPlay
                            loop
                            muted
                            playsInline
                        />
                    </div>
                );
            case 1:
                return (
                    <div className={styles.content}>
                        <p className={styles.description}>
                            After calibrating the eye tracker, you will watch a driving video.
                            Stay seated and <b>press the spacebar</b> <svg viewBox="0 0 400 200" className={styles.keyboard}>
                                <rect x="50" y="40" width="300" height="120" rx="10" fill="#2f3640" filter="url(#shadow)" />
                                <g fill="#3f4853">
                                    <rect x="70" y="55" width="30" height="30" rx="4" />
                                    <rect x="105" y="55" width="30" height="30" rx="4" />
                                    <rect x="140" y="55" width="30" height="30" rx="4" />
                                    <rect x="175" y="55" width="30" height="30" rx="4" />
                                    <rect x="210" y="55" width="30" height="30" rx="4" />
                                    <rect x="245" y="55" width="30" height="30" rx="4" />
                                    <rect x="280" y="55" width="30" height="30" rx="4" />
                                </g>
                                <g className="spacebar">
                                    <rect x="100" y="100" width="200" height="40" rx="4" fill="#3f4853">
                                        <animate
                                            attributeName="y"
                                            values="100;105;100"
                                            dur="2s"
                                            repeatCount="indefinite"
                                            begin="0s"
                                        />
                                        <animate
                                            attributeName="fill"
                                            values="#3f4853;#2d3436;#3f4853"
                                            dur="2s"
                                            repeatCount="indefinite"
                                            begin="0s"
                                        />
                                    </rect>
                                </g>
                                <defs>
                                    <filter id="shadow" x="-20%" y="-20%" width="140%" height="140%">
                                        <feDropShadow dx="0" dy="4" stdDeviation="3" floodColor="#000" floodOpacity="0.3" />
                                    </filter>
                                </defs>
                            </svg>to start recording a
                            potential hazard, then <b>press it again</b> when the hazard is no
                            longer present.
                        </p>
                        <div className="relative">
                            <video
                                src={simulationVideo}
                                className={`${styles.video} w-full`}
                                autoPlay
                                loop
                                muted
                                playsInline
                            />
                        </div>
                    </div>
                );
            case 2:
                return (
                    <div className={styles.content}>
                        <p className={styles.description}>
                            After reviewing the driving footage, you will answer{" "}
                            <b>a couple of questions</b> about the video you just watched.
                        </p>
                        <video
                            src={questionnaireTutorial}
                            className={styles.video}
                            autoPlay
                            loop
                            muted
                            playsInline
                        />
                    </div>
                );
            case 3:
                return (
                    <div className={`${styles.content} ${styles.finalSlide}`}>
                        <p className={styles.description}>
                            Participate in as many driving simulations as possible! Each
                            completed simulation earns you one raffle entry, and every
                            successful <b>referral grants you 10 entries.</b> The grand prize
                            winner will receive <b>$100!</b>
                        </p>
                        <p className={styles.description}>Your referral link can be found by clicking on the <i
                            className="bi bi-person-circle"
                            style={{ fontSize: '22px', color: '#2d3748' }}
                        ></i> icon in the top-right corner of your screen.</p>
                        <p className={styles.description}>
                            Each survey takes approximately <b>30-45 seconds</b> to complete.
                        </p>
                        <p className={styles.description}>
                            Thank you for participating in the <b>Human-Aligned Hazardous Dectection Survey</b>. Your contributions are
                            valuable to our research!
                        </p>
                        <Button
                            variant="dark"
                            onClick={handleBeginCalibration}
                            className={styles.button}
                        >
                            Begin Calibration
                        </Button>
                    </div>
                );
            default:
                return null;
        }
    };

    return (
        <>
            <HahdHeader />
            <Container fluid className={styles.container}>
            <Profile />

            {/* Stepper Component */}
            <Box sx={{ width: "100%", mb: 3 }}>
                <Stepper activeStep={activeStep} alternativeLabel>
                    {steps.map((label, index) => (
                        <Step key={label}>
                            <StepLabel
                                slots={{
                                    stepIcon: (iconProps) => (
                                        <StepIcon
                                            {...iconProps}
                                            sx={{
                                                '&.MuiStepIcon-root': {
                                                    color: activeStep > index
                                                        ? '#FFD700'
                                                        : activeStep === index
                                                            ? '#2d3748'
                                                            : 'gray'
                                                }
                                            }}
                                        />
                                    )
                                }}
                            >
                                {label}
                            </StepLabel>
                        </Step>
                    ))}
                </Stepper>
            </Box>

            {/* Step Content */}
            <div className={styles.contentWrapper}>
                {/* Back Button */}
                <Button
                    variant="secondary"
                    disabled={activeStep === 0}
                    onClick={handleBack}
                    className={styles.backButton}
                >
                    <i className="bi bi-arrow-left"></i>
                </Button>

                <div className={styles.stepContent}>{renderStepContent(activeStep)}</div>

                {/* Next Button */}
                <Button
                    disabled={activeStep === steps.length - 1}
                    onClick={handleNext}
                    className={styles.nextButton}
                >
                    <i className="bi bi-arrow-right"></i>
                </Button>
            </div>
        </Container>
        </>
    );
};

export default LandingPage;