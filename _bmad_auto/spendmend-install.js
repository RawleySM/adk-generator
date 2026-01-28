#!/usr/bin/env node
/**
 * SpendMend DevAgent - BMAD Non-Interactive Installer
 *
 * Project: Databricks Unity Catalog Agent Framework
 * - Codebase/database read access
 * - Sandboxed code-gen in job_A
 * - Code validation and execution in job_B
 *
 * Uses prompt mocking to answer all installer questions automatically.
 *
 * Usage:
 *   node spendmend-install.js [target-directory]
 *
 * If no target directory is provided, uses the parent directory.
 */

const path = require('path');
const fs = require('fs-extra');
const chalk = require('chalk');

// ============================================================================
// SPENDMEND CONFIGURATION - All prompts answered here
// ============================================================================

const SPENDMEND_CONFIG = {
  // Core Configuration
  core: {
    user_name: 'SpendMend DevAgent',
    communication_language: 'English',
    document_output_language: 'English',
    output_folder: '_bmad-output',
  },

  // BMad Method Module Configuration
  bmm: {
    project_name: 'SpendMend Databricks Agent',
    user_skill_level: 'expert',
    planning_artifacts: '_bmad-output/planning-artifacts',
    implementation_artifacts: '_bmad-output/implementation-artifacts',
    project_knowledge: 'docs',
  },

  // Installation Settings
  installation: {
    modules: ['bmm', 'bmb'],  // BMad Method + BMad Builder
    ides: ['claude-code'],
  },
};

// ============================================================================
// MOCK PROMPTS - Intercept all interactive prompts
// ============================================================================

// Answer queue for prompts - keyed by question content patterns
const PROMPT_ANSWERS = {
  // Core prompts
  'What should agents call you': SPENDMEND_CONFIG.core.user_name,
  'language should agents use': SPENDMEND_CONFIG.core.communication_language,
  'document output language': SPENDMEND_CONFIG.core.document_output_language,
  'output files be saved': SPENDMEND_CONFIG.core.output_folder,

  // BMM prompts
  'project called': SPENDMEND_CONFIG.bmm.project_name,
  'development experience level': SPENDMEND_CONFIG.bmm.user_skill_level,
  'planning artifacts': SPENDMEND_CONFIG.bmm.planning_artifacts,
  'implementation artifacts': SPENDMEND_CONFIG.bmm.implementation_artifacts,
  'project knowledge': SPENDMEND_CONFIG.bmm.project_knowledge,

  // Confirmation prompts - always accept defaults
  'Accept Defaults': true,
  'Install to this directory': true,
  'Create directory': true,
};

function findAnswer(message) {
  const msgLower = (message || '').toLowerCase();
  for (const [pattern, answer] of Object.entries(PROMPT_ANSWERS)) {
    if (msgLower.includes(pattern.toLowerCase())) {
      return { found: true, answer };
    }
  }
  return { found: false };
}

// Create mock prompts module
const mockPrompts = {
  async getClack() {
    return {};
  },

  async handleCancel() {
    return false;
  },

  async intro(message) {
    console.log(chalk.cyan(message));
  },

  async outro(message) {
    console.log(chalk.green(message));
  },

  async note(message, title) {
    if (title) console.log(chalk.cyan(`[${title}]`));
    console.log(chalk.dim(message));
  },

  async spinner() {
    return {
      start: (msg) => console.log(chalk.dim(`⠋ ${msg}`)),
      stop: () => {},
      message: (msg) => console.log(chalk.dim(`⠋ ${msg}`)),
    };
  },

  async select(options) {
    const { found, answer } = findAnswer(options.message);
    if (found) {
      console.log(chalk.dim(`  → ${options.message}: ${answer}`));
      return answer;
    }
    // Return default or first option
    const result = options.default || (options.choices && options.choices[0]?.value);
    console.log(chalk.dim(`  → ${options.message}: ${result} (default)`));
    return result;
  },

  async multiselect(options) {
    // Return initially checked items or defaults
    const result = options.initialValues ||
      (options.choices?.filter(c => c.checked).map(c => c.value)) ||
      [];
    console.log(chalk.dim(`  → ${options.message}: [${result.join(', ')}]`));
    return result;
  },

  async groupMultiselect(options) {
    const result = options.initialValues || [];
    console.log(chalk.dim(`  → ${options.message}: [${result.join(', ')}]`));
    return result;
  },

  async confirm(options) {
    const { found, answer } = findAnswer(options.message);
    if (found) {
      console.log(chalk.dim(`  → ${options.message}: ${answer ? 'Yes' : 'No'}`));
      return answer;
    }
    // Default to true for confirmations
    const result = options.default !== undefined ? options.default : true;
    console.log(chalk.dim(`  → ${options.message}: ${result ? 'Yes' : 'No'} (default)`));
    return result;
  },

  async text(options) {
    const { found, answer } = findAnswer(options.message);
    if (found) {
      console.log(chalk.dim(`  → ${options.message}: ${answer}`));
      return answer;
    }
    const result = options.default || options.placeholder || '';
    console.log(chalk.dim(`  → ${options.message}: ${result} (default)`));
    return result;
  },

  async password(options) {
    return options.default || '';
  },

  async group(prompts, options = {}) {
    const result = {};
    for (const [key, promptFn] of Object.entries(prompts)) {
      result[key] = await promptFn();
    }
    return result;
  },

  async tasks(taskList) {
    for (const task of taskList) {
      if (task.enabled === false) continue;
      console.log(chalk.dim(`  ⠋ ${task.title}`));
      await task.task();
    }
  },

  log: {
    info: (msg) => console.log(chalk.blue(`ℹ ${msg}`)),
    success: (msg) => console.log(chalk.green(`✓ ${msg}`)),
    warn: (msg) => console.log(chalk.yellow(`⚠ ${msg}`)),
    error: (msg) => console.log(chalk.red(`✗ ${msg}`)),
    message: (msg) => console.log(msg),
    step: (msg) => console.log(chalk.cyan(`→ ${msg}`)),
  },

  async prompt(questions) {
    const answers = {};
    for (const q of questions) {
      const { found, answer } = findAnswer(q.message);
      if (found) {
        answers[q.name] = answer;
        console.log(chalk.dim(`  → ${q.message}: ${answer}`));
      } else {
        answers[q.name] = typeof q.default === 'function' ? q.default(answers) : q.default;
        console.log(chalk.dim(`  → ${q.message}: ${answers[q.name]} (default)`));
      }
    }
    return answers;
  },
};

// ============================================================================
// INSTALLER RUNNER
// ============================================================================

async function runInstaller(targetDir, bmadPackageDir) {
  // Patch require cache with mock prompts
  const promptsPath = path.join(bmadPackageDir, 'tools/cli/lib/prompts.js');
  require.cache[require.resolve(promptsPath)] = {
    id: promptsPath,
    filename: promptsPath,
    loaded: true,
    exports: mockPrompts,
  };

  // Load installer (will use mocked prompts)
  const { Installer } = require(path.join(bmadPackageDir, 'tools/cli/installers/lib/core/installer.js'));
  const { MessageLoader } = require(path.join(bmadPackageDir, 'tools/cli/installers/lib/message-loader.js'));

  const installer = new Installer();

  // Check for existing installation
  const { bmadDir } = await installer.findBmadDir(targetDir);
  const hasExisting = await fs.pathExists(bmadDir);

  if (hasExisting) {
    console.log(chalk.yellow('Existing installation detected - performing quick update...\n'));

    const config = {
      actionType: 'quick-update',
      directory: targetDir,
      customContent: { hasCustomContent: false },
    };

    const result = await installer.quickUpdate(config);
    console.log(chalk.green('\n✨ Quick update complete!'));
    console.log(chalk.cyan(`Updated ${result.moduleCount} modules (${result.modules.join(', ')})`));

  } else {
    console.log(chalk.yellow('Installing BMAD with SpendMend configuration...\n'));

    const config = {
      actionType: 'install',
      directory: targetDir,
      installCore: true,
      modules: SPENDMEND_CONFIG.installation.modules,
      ides: SPENDMEND_CONFIG.installation.ides,
      skipIde: false,
      coreConfig: SPENDMEND_CONFIG.core,
      customContent: { hasCustomContent: false },
    };

    const result = await installer.install(config);

    if (result && result.success) {
      console.log(chalk.green('\n✨ BMAD installation complete!'));
    } else if (result && result.cancelled) {
      console.log(chalk.yellow('\nInstallation was cancelled.'));
      process.exit(1);
    }
  }

  // Display end message
  const messageLoader = new MessageLoader();
  messageLoader.displayEndMessage();
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  const scriptDir = __dirname;
  const targetDir = process.argv[2] || path.resolve(scriptDir, '..');

  console.log(chalk.cyan.bold('\n' + '='.repeat(80)));
  console.log(chalk.cyan.bold('  SpendMend DevAgent - BMAD Non-Interactive Installation'));
  console.log(chalk.cyan.bold('  Databricks Unity Catalog Agent Framework'));
  console.log(chalk.cyan.bold('='.repeat(80) + '\n'));

  console.log(chalk.white('Configuration:'));
  console.log(chalk.dim(`  Target:    ${targetDir}`));
  console.log(chalk.dim(`  User/Team: ${SPENDMEND_CONFIG.core.user_name}`));
  console.log(chalk.dim(`  Modules:   ${SPENDMEND_CONFIG.installation.modules.join(', ')}`));
  console.log(chalk.dim(`  IDE:       ${SPENDMEND_CONFIG.installation.ides.join(', ')}`));
  console.log('');

  try {
    // Download and extract bmad-method package if not cached
    const cacheDir = path.join(scriptDir, '.cache');
    const packageDir = path.join(cacheDir, 'package');

    if (!await fs.pathExists(path.join(packageDir, 'package.json'))) {
      console.log(chalk.yellow('Downloading bmad-method package...\n'));

      await fs.ensureDir(cacheDir);

      // Use npm pack to download the package
      const { execSync } = require('child_process');
      execSync('npm pack bmad-method --pack-destination .cache', {
        cwd: scriptDir,
        stdio: 'inherit'
      });

      // Extract the tarball
      const tarballs = await fs.readdir(cacheDir);
      const tarball = tarballs.find(f => f.startsWith('bmad-method') && f.endsWith('.tgz'));
      if (tarball) {
        execSync(`tar -xzf "${tarball}"`, { cwd: cacheDir });
        await fs.remove(path.join(cacheDir, tarball));
      }

      // Install dependencies
      console.log(chalk.yellow('Installing dependencies...\n'));
      execSync('npm install --omit=dev --ignore-scripts', {
        cwd: packageDir,
        stdio: 'inherit'
      });
    }

    await runInstaller(targetDir, packageDir);

    // Display project summary
    console.log(chalk.cyan('\n' + '─'.repeat(80)));
    console.log(chalk.cyan.bold('SpendMend DevAgent - Ready'));
    console.log(chalk.cyan('─'.repeat(80)));
    console.log(`
${chalk.white('Architecture:')} Databricks Unity Catalog Agent Framework

${chalk.white('Job A - Code Generation (Sandboxed):')}
  • Codebase read access via Unity Catalog
  • Database schema introspection
  • Data pipeline spec generation
  • Security architecture templates

${chalk.white('Job B - Validation & Execution:')}
  • Code validation and linting
  • Security policy compliance checks
  • Execution in controlled environment
  • Audit trail generation

${chalk.white('BMAD Commands Available:')}
  • /bmad      - Main orchestrator
  • /architect - System design
  • /pm        - Project management
  • /dev       - Implementation

${chalk.white('Next Steps:')}
  1. Open this folder in Claude Code
  2. Run /bmad to start planning
  3. Begin with architecture design
`);

  } catch (error) {
    console.error(chalk.red('\nInstallation failed:'), error.message);
    if (process.env.DEBUG || process.argv.includes('--debug')) {
      console.error(chalk.dim(error.stack));
    }
    process.exit(1);
  }
}

main();
